import base64
import datetime as dt
import gzip
import json
import os
import re
import time
from pathlib import Path
from typing import Literal, Union

import branca
import dask_geopandas as dg
import folium
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from calitp_data_analysis import geography_utils, get_fs, utils
from calitp_data_analysis.tables import tbls
from numba import jit
from shared_utils import gtfs_utils_v2, rt_dates
from siuba import *

fs = get_fs()

# set system time
os.environ["TZ"] = "America/Los_Angeles"
time.tzset()

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/rt_delay"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"
EXPORT_PATH = f"{GCS_FILE_PATH}cached_views/"
SHN_PATH = "gs://calitp-analytics-data/data-analyses/bus_service_increase/highways.parquet"
VP_FILE_PATH = f"gs://{BUCKET_NAME}/data-analyses/rt_segment_speeds/"
V2_SUBFOLDER = "v2_cached_views/"

SPA_MAP_SITE = "https://embeddable-maps.calitp.org/"
SPA_MAP_BUCKET = "calitp-map-tiles/"
SPEEDMAP_LEGEND_URL = "https://storage.googleapis.com/calitp-map-tiles/speeds_legend.svg"

MPH_PER_MPS = 2.237  # use to convert meters/second to miles/hour
METERS_PER_MILE = 1609.34
# Colorscale
ZERO_THIRTY_COLORSCALE = branca.colormap.step.RdYlGn_10.scale(vmin=0, vmax=30)
ZERO_THIRTY_COLORSCALE.caption = "Speed (miles per hour)"
VARIANCE_COLORS = branca.colormap.step.Blues_06.colors[1:]  # actual breaks will vary
VARIANCE_RANGE = np.arange(1, 2.8, 0.3)
VARIANCE_FIXED_COLORSCALE = branca.colormap.StepColormap(
    colors=VARIANCE_COLORS, index=VARIANCE_RANGE, vmin=min(VARIANCE_RANGE), vmax=max(VARIANCE_RANGE)
)
VARIANCE_FIXED_COLORSCALE.caption = "80th percentile to 20th percentile speed ratio (variation in speeds)"

# Datetime formats
DATE_WEEKDAY_FMT = "%b %d (%a)"  # Jun 01 (Wed) for 6/1/22
MONTH_DAY_FMT = "%m_%d"  # 6_01 for 6/1/22
HOUR_MIN_FMT = "%H:%M"  # 08:00 for 8 am, 13:00 for 1pm
HOUR_MIN_SEC_FMT = "%H:%M:%S"  # 08:15:05 for 8:15 am + 5 sec, 13:15:05 for 1:15pm + 5 sec
FULL_DATE_FMT = "%Y-%m-%d"  # 2022-06-01 for 6/1/22

# decide to use v1 cached data from gcs or v2 warehouse cached data/fresh queries
warehouse_cutoff_date = dt.date(2022, 12, 31)

trip_cols = [
    "feed_key",
    "trip_key",
    "gtfs_dataset_key",
    "service_date",
    "trip_id",
    "route_id",
    "route_short_name",
    "shape_id",
    "direction_id",
    "route_type",
    "route_long_name",
    "route_desc",
]

st_cols = [
    "feed_key",
    "trip_id",
    "stop_id",
    "arrival_time",
    "departure_time",
    "timepoint",
    "stop_sequence",
    "continuous_drop_off",
    "continuous_pickup",
    "arrival_sec",
    "departure_sec",
]
# must include _sec for util to work...
stop_cols = ["feed_key", "stop_id", "stop_name", "pt_geom"]
# must include pt_geom to return gdf
shape_cols = ["feed_key", "shape_id"]


# used in gtfs_utils
def format_date(analysis_date: Union[dt.date, str]) -> str:
    """
    Get date formatted correctly in all the queries
    """
    if isinstance(analysis_date, dt.date):
        return analysis_date.strftime(FULL_DATE_FMT)
    elif isinstance(analysis_date, str):
        return dt.datetime.strptime(analysis_date, FULL_DATE_FMT).date()


def reversed_colormap(existing: branca.colormap.ColorMap) -> branca.colormap.ColorMap:
    return branca.colormap.LinearColormap(
        colors=list(reversed(existing.colors)), vmin=existing.vmin, vmax=existing.vmax
    )


def cardinal_definition_rules(distance_east: float, distance_north: float) -> str:
    if abs(distance_east) > abs(distance_north):
        if distance_east > 0:
            return "Eastbound"
        elif distance_east < 0:
            return "Westbound"
        else:
            return "Unknown"
    else:
        if distance_north > 0:
            return "Northbound"
        elif distance_north < 0:
            return "Southbound"
        else:
            return "Unknown"


def primary_cardinal_direction(
    origin: shapely.geometry.Point,
    destination: shapely.geometry.Point,
) -> str:
    """
    origin: shapely.geometry.Point
    destination: shapely.geometry.Point

    Takes point geometry and returns the primary cardinal direction
    (north, south, east, west). To use, first grab the origin
    and destination of a line geometry.
    """
    distance_east = destination.x - origin.x
    distance_north = destination.y - origin.y

    return cardinal_definition_rules(distance_east, distance_north)


def add_origin_destination(
    gdf: Union[gpd.GeoDataFrame, dg.GeoDataFrame],
) -> Union[gpd.GeoDataFrame, dg.GeoDataFrame]:
    """
    For a gdf, add the origin, destination columns given a linestring.
    Note: multilinestring may not work!
    https://gis.stackexchange.com/questions/358584/how-to-extract-long-and-lat-of-start-and-end-points-to-seperate-columns-from-t
    """
    if isinstance(gdf, dg.GeoDataFrame):
        gdf = gdf.assign(
            origin=gdf.geometry.apply(
                lambda x: shapely.geometry.Point(x.coords[0]),
                meta=("origin", "geometry"),
            ),
            destination=gdf.geometry.apply(
                lambda x: shapely.geometry.Point(x.coords[-1]),
                meta=("destination", "geometry"),
            ),
        )

    elif isinstance(gdf, gpd.GeoDataFrame):
        gdf = gdf.assign(
            origin=gdf.geometry.apply(lambda x: shapely.geometry.Point(x.coords[0])),
            destination=gdf.geometry.apply(lambda x: shapely.geometry.Point(x.coords[-1])),
        )

    return gdf


direction_grouping = {
    "Northbound": "north-south",
    "Southbound": "north-south",
    "Eastbound": "east-west",
    "Westbound": "east-west",
}


def add_route_cardinal_direction(
    df: Union[gpd.GeoDataFrame, dg.GeoDataFrame],
    origin: str = "origin",
    destination: str = "destination",
) -> Union[gpd.GeoDataFrame, dg.GeoDataFrame]:
    """
    Apply cardinal direction to gdf.

    Returns gdf with new columns: route_primary_direction and route_direction

       route_primary_direction: Northbound, Southbound, Eastbound, Westbound
       route_direction: north-south, east-west
    """

    # Stick the origin/destination of a route_id and return the primary cardinal direction
    if isinstance(df, dg.GeoDataFrame):
        df = df.assign(
            route_primary_direction=df.apply(
                lambda x: primary_cardinal_direction(x[origin], x[destination]),
                axis=1,
                meta=("route_direction", "str"),
            )
        )

    elif isinstance(df, gpd.GeoDataFrame):
        df = df.assign(
            route_primary_direction=df.apply(lambda x: primary_cardinal_direction(x[origin], x[destination]), axis=1)
        )

    # In cases where you don't care exactly if it's southbound or northbound,
    # but care that it's north-south, such as
    # testing for orthogonality of 2 bus routes intersecting
    df = df.assign(
        route_direction=df.route_primary_direction.map(direction_grouping),
    )

    return df


def show_full_df(df: pd.DataFrame):
    with pd.option_context("display.max_rows", None):
        return display(df)


def fix_arrival_time(gtfs_timestring: str) -> tuple[str, int]:
    """Reformats a GTFS timestamp (which allows the hour to exceed
    24 to mark service day continuity)
    to standard 24-hour time.
    """
    extra_day = 0
    if not gtfs_timestring:  # preserve none if time not provided
        return None, extra_day
    split = gtfs_timestring.split(":")
    hour = int(split[0])

    if hour >= 24:
        extra_day = 1
        split[0] = str(hour - 24)
        corrected = (":").join(split)
        return corrected.strip(), extra_day

    else:
        return gtfs_timestring.strip(), extra_day


# TODO use?
def gtfs_time_to_dt(df: pd.DataFrame) -> pd.DataFrame:
    date = df.service_date

    timestring, extra_day = fix_arrival_time(df.arrival_time)

    df["arrival_dt"] = dt.datetime.combine(
        date + dt.timedelta(days=extra_day),
        dt.datetime.strptime(timestring, HOUR_MIN_SEC_FMT).time(),
    )

    return df


def interpolate_arrival_times(df):
    """
    Interpolate between provided arrival_times linearly with distance
    """
    interp_df = df.dropna(subset=["arrival_time"])
    yp = interp_df.arrival_time.to_numpy()
    yp = yp.astype("datetime64[s]").astype("float64")
    xp = interp_df.shape_meters.to_numpy()

    def interpolator(x, shape_meters_array, arrival_time_np):
        return np.interp(x, shape_meters_array, arrival_time_np)

    df = df.assign(
        arrival_time=df.apply(
            lambda x: interpolator(x.shape_meters, xp, yp) if pd.isnull(x.arrival_time) else x.arrival_time,
            axis=1,
        )
    )

    df["arrival_time"] = df.arrival_time.to_numpy().astype("datetime64[s]")

    return df


def check_cached(
    filename: str,
    GCS_FILE_PATH: Union[str, Path] = GCS_FILE_PATH,
    subfolder: Union[str, Path] = "cached_views/",
) -> Union[str, Path]:
    """
    Check GCS bucket to see if a file already is there.
    Returns the path, if it exists.

    GCS_FILE_PATH: Defaults to gs://calitp-analytics-data/data-analyses/rt_delay/
    """
    path = f"{GCS_FILE_PATH}{subfolder}{filename}"
    if fs.exists(path):
        return path
    else:
        return None


def trips_cached(itp_id: int, date_str: str) -> pd.DataFrame:
    """
    Check for the trips file for that operator on selected date.
    Use in all the queries that can take a trips pd.DataFrame.

    If not, return None, and a fresh query can be run.
    """
    trips_cached = check_cached(f"trips_{itp_id}_{date_str}.parquet")

    if trips_cached:
        return pd.read_parquet(trips_cached)
    else:
        return None


def get_speedmaps_ix_df(analysis_date: dt.date, itp_id: Union[int, None] = None) -> pd.DataFrame:
    """
    Collect relevant keys for finding all schedule and rt data for a reports-assessed organization.
    Note that organizations may have multiple sets of feeds, or share feeds with other orgs.

    Used with specific itp_id in rt_analysis.rt_parser.OperatorDayAnalysis, or without specifying itp_id
    to get an overall table of which datasets were processed and how to deduplicate if needed
    """
    analysis_dt = dt.datetime.combine(analysis_date, dt.time(0))

    daily_service = tbls.mart_gtfs.fct_daily_feed_scheduled_service_summary() >> select(
        _.schedule_gtfs_dataset_key == _.gtfs_dataset_key, _.feed_key, _.service_date
    )

    dim_orgs = (
        tbls.mart_transit_database.dim_organizations()
        >> filter(_._valid_from <= analysis_dt, _._valid_to > analysis_dt)
        >> select(_.source_record_id, _.caltrans_district)
    )

    org_feeds_datasets = (
        tbls.mart_transit_database.dim_provider_gtfs_data()
        >> filter(_._valid_from <= analysis_dt, _._valid_to >= analysis_dt)
        >> filter(
            _.public_customer_facing_or_regional_subfeed_fixed_route, _.vehicle_positions_gtfs_dataset_key != None
        )
        >> inner_join(_, daily_service, by="schedule_gtfs_dataset_key")
        >> inner_join(_, dim_orgs, on={"organization_source_record_id": "source_record_id"})
        >> filter(_.service_date == analysis_date)
        >> select(
            _.feed_key,
            _.schedule_gtfs_dataset_key,
            _.vehicle_positions_gtfs_dataset_key,
            _.organization_itp_id,
            _.caltrans_district,
            _.organization_name,
            _.service_date,
        )
    )

    if itp_id:
        org_feeds_datasets = org_feeds_datasets >> filter(_.organization_itp_id == itp_id)

    return org_feeds_datasets >> collect()


def compose_filename_check(ix_df: pd.DataFrame, table: str):
    """
    Compose target filename for cached warehouse queries (as used in rt_analysis.rt_parser.OperatorDayAnalysis),
    then check if cached data already exists on GCS
    """
    service_date = ix_df.service_date.iloc[0].date()
    date_str = service_date.strftime(FULL_DATE_FMT)
    assert date_str in rt_dates.DATES.values(), "selected date not in rt_dates"
    filename = f"{table}_{ix_df.organization_itp_id.iloc[0]}_{date_str}.parquet"
    path = check_cached(filename=filename, subfolder=V2_SUBFOLDER)

    return filename, path, service_date


def get_vehicle_positions(ix_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Using ix_df as a guide, download all-operator data from GCS as queried via:
        https://github.com/cal-itp/data-analyses/blob/main/open_data/download_vehicle_positions.py
    Filter to relevant vehicle positions datasets; cache filtered version
    """

    filename, path, service_date = compose_filename_check(ix_df, "vp")
    date_str = service_date.isoformat()

    if path:
        print(f"found vp parquet at {path}")
        org_vp = gpd.read_parquet(path)
    else:
        vp_all = gpd.read_parquet(f"{VP_FILE_PATH}vp_{date_str}.parquet")
        org_vp = vp_all >> filter(_.gtfs_dataset_key.isin(ix_df.vehicle_positions_gtfs_dataset_key))
        org_vp = org_vp >> select(-_.location_timestamp, -_.service_date, -_.activity_date)
        org_vp = org_vp.to_crs(geography_utils.CA_NAD83Albers)
        utils.geoparquet_gcs_export(org_vp, GCS_FILE_PATH + V2_SUBFOLDER, filename)

    return org_vp


def get_trips(ix_df: pd.DataFrame) -> pd.DataFrame:
    """
    Using ix_df as a guide, query warehouse for trips via shared_utils.gtfs_utils_v2
    Only request columns used in speedmap workflow; cache
    """
    filename, path, service_date = compose_filename_check(ix_df, "trips")

    if path:
        print(f"found trips parquet at {path}")
        org_trips = pd.read_parquet(path)
    else:
        feed_key_list = list(ix_df.feed_key.unique())
        org_trips = gtfs_utils_v2.get_trips(service_date, feed_key_list, trip_cols)
        org_trips.to_parquet(GCS_FILE_PATH + V2_SUBFOLDER + filename)

    return org_trips


def get_st(ix_df: pd.DataFrame, trip_df: pd.DataFrame) -> pd.DataFrame:
    """
    Using ix_df as a guide, query warehouse for stop_times via shared_utils.gtfs_utils_v2
    Only request columns used in speedmap workflow; cache
    """
    filename, path, service_date = compose_filename_check(ix_df, "st")

    if path:
        print(f"found stop times parquet at {path}")
        org_st = pd.read_parquet(path)
    else:
        feed_key_list = list(ix_df.feed_key.unique())
        org_st = gtfs_utils_v2.get_stop_times(
            service_date, feed_key_list, trip_df=trip_df, stop_time_cols=st_cols, get_df=True
        )
        org_st = org_st >> select(-_.arrival_sec, -_.departure_sec)
        org_st.to_parquet(GCS_FILE_PATH + V2_SUBFOLDER + filename)

    return org_st


def get_stops(ix_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Using ix_df as a guide, query warehouse for stops via shared_utils.gtfs_utils_v2
    Only request columns used in speedmap workflow; cache
    """
    filename, path, service_date = compose_filename_check(ix_df, "stops")

    if path:
        print(f"found stops parquet at {path}")
        org_stops = gpd.read_parquet(path)
    else:
        feed_key_list = list(ix_df.feed_key.unique())
        org_stops = gtfs_utils_v2.get_stops(service_date, feed_key_list, stop_cols, crs=geography_utils.CA_NAD83Albers)
        utils.geoparquet_gcs_export(org_stops, GCS_FILE_PATH + V2_SUBFOLDER, filename)

    return org_stops


def get_shapes(ix_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Using ix_df as a guide, query warehouse for shapes via shared_utils.gtfs_utils_v2
    Only request columns used in speedmap workflow; cache
    """
    filename, path, service_date = compose_filename_check(ix_df, "shapes")

    if path:
        print(f"found shapes parquet at {path}")
        org_shapes = gpd.read_parquet(path)
    else:
        feed_key_list = list(ix_df.feed_key.unique())
        org_shapes = gtfs_utils_v2.get_shapes(
            service_date, feed_key_list, crs=geography_utils.CA_NAD83Albers, shape_cols=shape_cols
        )
        # invalid geos are nones in new df...
        org_shapes = org_shapes.dropna(subset=["geometry"])
        assert isinstance(org_shapes, gpd.GeoDataFrame) and not org_shapes.empty, "shapes must not be empty"
        utils.geoparquet_gcs_export(org_shapes, GCS_FILE_PATH + V2_SUBFOLDER, filename)

    return org_shapes


# needed for v1 RtFilterMapper compatibility
def get_routelines(
    itp_id: int,
    analysis_date: dt.date,
    force_clear: bool = False,
    export_path: Union[str, Path] = EXPORT_PATH,
) -> gpd.GeoDataFrame:
    date_str = analysis_date.strftime(FULL_DATE_FMT)
    filename = f"routelines_{itp_id}_{date_str}.parquet"

    path = check_cached(filename)

    if path and not force_clear:
        print("found parquet")
        cached = gpd.read_parquet(path)
        if not cached.empty:
            return cached
        else:
            print("v1 cached parquet empty -- unable to generate")
            return

        return routelines


def check_intermediate_data(
    speedmaps_index_df: pd.DataFrame = pd.DataFrame(),
    analysis_date: dt.date = None,
) -> pd.DataFrame:
    """
    speedmaps_index_df: pd.DataFrame of all agencies to try generating a speedmap
        from rt_delay/build_speedmaps_index.py
    For speedmap generation scripts in rt_delay.
    Check if intermediate file exists (process partially complete) and
    return that to script, otherwise check intermediate data from GCS
    """
    assert analysis_date or not speedmaps_index_df.empty, "must provide analysis date if not providing index df"
    analysis_date = speedmaps_index_df.analysis_date.iloc[0] if not analysis_date else analysis_date
    progress_path = f"./_rt_progress_{analysis_date}.parquet"
    already_tried = os.path.exists(progress_path)
    assert already_tried or not speedmaps_index_df.empty, "must provide df if no existing progress parquet"
    if already_tried:
        print(f"found {progress_path}, resuming")
        speedmaps_index_joined = pd.read_parquet(progress_path)
        print(speedmaps_index_joined >> count(_.status))  # show status when running script
    else:
        operators_ran = get_operators(analysis_date, speedmaps_index_df.organization_itp_id.to_list())
        operators_ran_df = pd.DataFrame.from_dict(operators_ran, orient="index", columns=["status"])
        operators_ran_df.index.name = "itp_id"
        speedmaps_index_joined = speedmaps_index_df >> inner_join(
            _, operators_ran_df, on={"organization_itp_id": "itp_id"}
        )

    return speedmaps_index_joined


def categorize_time_of_day(value: Union[int, dt.datetime]) -> str:
    if isinstance(value, int):
        hour = value
    elif isinstance(value, dt.datetime):
        hour = value.hour
    if hour < 4:
        return "Owl"
    elif hour < 7:
        return "Early AM"
    elif hour < 10:
        return "AM Peak"
    elif hour < 15:
        return "Midday"
    elif hour < 20:
        return "PM Peak"
    else:
        return "Evening"


@jit(nopython=True)  # numba gives huge speedup here (~60x)
def time_at_position_numba(desired_position, shape_array, dt_float_array):
    if desired_position < shape_array.max() and desired_position > shape_array.min():
        return np.interp(desired_position, shape_array, dt_float_array)
    else:
        return None


def try_parallel(geometry):
    try:
        return geometry.parallel_offset(30, "right")
    except Exception:
        return geometry


def arrowize_segment(line_geometry, buffer_distance: int = 20):
    """Given a linestring segment from a gtfs shape,
    buffer and clip to show direction of progression"""
    arrow_distance = buffer_distance  # was buffer_distance * 0.75
    st_end_distance = arrow_distance + 3  # avoid floating point errors...
    try:
        segment = line_geometry.simplify(tolerance=5)
        if segment.length < 50:  # return short segments unmodified, for now
            return segment.buffer(buffer_distance)
        arrow_distance = max(arrow_distance, line_geometry.length / 20)
        shift_distance = buffer_distance + 1

        begin_segment = shapely.ops.substring(segment, 0, st_end_distance)
        r_shift = begin_segment.parallel_offset(shift_distance, "right")
        r_pt = shapely.ops.substring(r_shift, 0, 0)
        l_shift = begin_segment.parallel_offset(shift_distance, "left")
        l_pt = shapely.ops.substring(l_shift, 0, 0)
        end = shapely.ops.substring(
            begin_segment,
            begin_segment.length + arrow_distance,
            begin_segment.length + arrow_distance,
        )
        poly = shapely.geometry.Polygon((r_pt, end, l_pt))  # triangle to cut bottom of arrow
        # ends to the left
        end_segment = shapely.ops.substring(segment, segment.length - st_end_distance, segment.length)
        end = shapely.ops.substring(end_segment, end_segment.length, end_segment.length)  # correct
        r_shift = end_segment.parallel_offset(shift_distance, "right")
        r_pt = shapely.ops.substring(r_shift, 0, 0)
        r_pt2 = shapely.ops.substring(r_shift, r_shift.length - arrow_distance, r_shift.length - arrow_distance)
        l_shift = end_segment.parallel_offset(shift_distance, "left")
        l_pt = shapely.ops.substring(l_shift, 0, 0)
        l_pt2 = shapely.ops.substring(l_shift, arrow_distance, arrow_distance)
        t1 = shapely.geometry.Polygon((l_pt2, end, l_pt))  # triangles to cut top of arrow
        t2 = shapely.geometry.Polygon((r_pt2, end, r_pt))

        segment_clip_mask = shapely.geometry.MultiPolygon((poly, t1, t2))

        # buffer, then clip segment with arrow shape
        differences = segment.buffer(buffer_distance).difference(segment_clip_mask)
        # # of resulting geometries, pick largest (actual segment, not any scraps...)
        areas = [x.area for x in differences.geoms]
        for geom in differences.geoms:
            if geom.area == max(areas):
                return geom
    except Exception:
        return line_geometry.simplify(tolerance=5).buffer(buffer_distance)


def arrowize_by_frequency(row, frequency_col="trips_per_hour", frequency_thresholds=(1.5, 3, 6)):
    if row[frequency_col] < frequency_thresholds[0]:
        row.geometry = arrowize_segment(row.geometry, buffer_distance=15)
    elif row[frequency_col] < frequency_thresholds[1]:
        row.geometry = arrowize_segment(row.geometry, buffer_distance=20)
    elif row[frequency_col] < frequency_thresholds[2]:
        row.geometry = arrowize_segment(row.geometry, buffer_distance=25)
    else:
        row.geometry = arrowize_segment(row.geometry, buffer_distance=30)
    return row


def layer_points(rt_interpolator):
    keep_cols = [
        "geometry",
        "shape_meters",
        "progressed",
        "secs_from_last",
        "meters_from_last",
    ]

    initial_bk_noise = rt_interpolator.position_gdf >> filter(_.meters_from_last < 0) >> select(*keep_cols)

    initial_deduped = rt_interpolator.position_gdf >> distinct(_.shape_meters, _keep_all=True) >> select(*keep_cols)

    cleaned = rt_interpolator.cleaned_positions >> select(*keep_cols)

    popup_dict = {
        "shape_meters": "shape_meters",
        "progressed": "progressed",
        "secs_from_last": "secs_from_last",
        "meters_from_last": "meters_from_last",
    }

    layers_dict = {
        "initial backwards noise": {
            "df": initial_bk_noise,
            "plot_col": "shape_meters",
            "popup_dict": popup_dict,
            "tooltip_dict": popup_dict,
            "colorscale": branca.colormap.step.Blues_03,
        },
        "initial position deduplicated": {
            "df": initial_deduped,
            "plot_col": "shape_meters",
            "popup_dict": popup_dict,
            "tooltip_dict": popup_dict,
            "colorscale": branca.colormap.step.Greens_03,
        },
        "cleaned_final": {
            "df": cleaned,
            "plot_col": "shape_meters",
            "popup_dict": popup_dict,
            "tooltip_dict": popup_dict,
            "colorscale": branca.colormap.step.Greens_03,
        },
    }

    for i in range(rt_interpolator._position_cleaning_count):
        layers_dict[f"cleaned_{i}"] = {
            "df": (rt_interpolator.debug_dict[f"clean_{i}"] >> select(*keep_cols)),
            "plot_col": "shape_meters",
            "popup_dict": popup_dict,
            "tooltip_dict": popup_dict,
            "colorscale": branca.colormap.step.Reds_03,
            # 'marker':  marker
        }
    # return layers_dict
    # TODO: fix multiple layer plotting using gdf.explore()
    # return map_utils.make_folium_multiple_layers_map(layers_dict, 900, 500)


def map_line(gdf):
    # gdf = gdf.buffer(1)
    gdf = gdf.to_crs(geography_utils.WGS84)
    centroid = gdf.geometry.iloc[0].centroid
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=13, tiles="cartodbpositron")

    folium.GeoJson(gdf.to_json()).add_to(m)

    return m


def categorize_cleaning(rt_operator_day, interpolator_key):
    rt_interpolator = rt_operator_day.position_interpolators[interpolator_key]["rt"]
    raw = rt_interpolator.position_gdf.shape[0]
    same_loc_dropped = (rt_interpolator.position_gdf >> distinct(_.shape_meters)).shape[0]
    cleaned = rt_interpolator.cleaned_positions.shape[0]

    return (interpolator_key, cleaned / raw, cleaned / same_loc_dropped)


def exclude_desc(desc):
    # match descriptions that don't give additional info, like Route 602 or Route 51B
    exclude_texts = [
        " *Route *[0-9]*[a-z]{0,1}$",
        " *Metro.*(Local|Rapid|Limited).*Line",
        " *(Redwood Transit serves the communities of|is operated by Eureka Transit and serves)",
        " *service within the Stockton Metropolitan Area",
        " *Hopper bus can deviate",
        " *RTD's Interregional Commuter Service is a limited-capacity service",
    ]
    desc_eval = [re.search(text, desc, flags=re.IGNORECASE) for text in exclude_texts]
    # number_only = re.search(' *Route *[0-9]*[a-z]{0,1}$', desc, flags=re.IGNORECASE)
    # metro = re.search(' *Metro.*(Local|Rapid|Limited).*Line', desc, flags=re.IGNORECASE)
    # redwood = re.search(' *(Redwood Transit serves the communities of|is operated by Eureka Transit and serves)', desc, flags=re.IGNORECASE)
    # return number_only or metro or redwood
    return any(desc_eval)


def which_desc(row):
    long_name_valid = row.route_long_name and not exclude_desc(row.route_long_name)
    route_desc_valid = row.route_desc and not exclude_desc(row.route_desc)
    if route_desc_valid:
        return f", {row.route_desc}"
    elif long_name_valid:
        return f", {row.route_long_name}"
    else:
        return ""


def describe_most_delayed(row):
    description = which_desc(row)
    full_description = (
        f"{row.route_short_name}{description}, {row.direction}: "
        f"{round(row.mean_delay_seconds / 60, 0)} minutes late on average"
    )
    row["full_description"] = full_description
    return row


def describe_slowest(row):
    description = which_desc(row)
    full_description = (
        f"{row.route_short_name}{description}, {row.direction}: "
        f"{round(row.median_trip_mph, 1)} mph median trip speed for "
        f'{row.num_trips} trip{"s" if row.num_trips > 1 else ""}'
    )
    row["full_description"] = full_description
    return row


route_type_names = {
    "0": "Tram, Streetcar, Light rail",
    "1": "Subway, Metro",
    "2": "Rail",
    "3": "Bus",
    "4": "Ferry",
    "5": "Cable tram",
    "6": "Aerial lift, suspended cable car",
    "7": "Funicular",
    "11": "Trolleybus",
    "12": "Monorail",
}


def get_operators(analysis_date, operator_list, verbose=False):
    """
    Function for checking the existence of rt_trips and stop_delay_views in GCS for operators on a given day.

    analysis_date: datetime.date
    operator_list: list of itp_id's
    verbose: print status in additon to returning dict
    """

    if isinstance(analysis_date, str):
        analysis_date = pd.to_datetime(analysis_date).date()
    # if analysis_date <= dt.date(2022, 12, 31):  # look for v1 or v2 intermediate data
    #     subfolder = "rt_trips/"
    # else:
    subfolder = "v2_rt_trips/"
    fs_list = fs.ls(f"{GCS_FILE_PATH}{subfolder}")
    # day = str(analysis_date.day).zfill(2)
    # month = str(analysis_date.month).zfill(2)
    date_iso = analysis_date.isoformat()
    # now finds ran operators on specific analysis date
    ran_operators = [
        int(path.split(f"{subfolder}")[1].split("_")[0])
        for path in fs_list
        if date_iso in path.split(f"{subfolder}")[1]
    ]
    op_list_runstatus = {}
    for itp_id in operator_list:
        if itp_id in ran_operators:
            if verbose:
                print(f"already ran: {itp_id}")
            op_list_runstatus[itp_id] = "already_ran"
            continue
        else:
            if verbose:
                print(f"not yet run: {itp_id}")
            op_list_runstatus[itp_id] = "not_yet_run"
    return op_list_runstatus


def spa_map_export_link(
    gdf: gpd.GeoDataFrame, path: str, state: dict, site: str = SPA_MAP_SITE, cache_seconds: int = 3600
):
    """
    Called via set_state_export. Handles stream writing of gzipped geojson to GCS bucket,
    encoding spa state as base64 and URL generation.
    """
    assert cache_seconds in range(3601), "cache must be 0-3600 seconds"
    geojson_str = gdf.to_json()
    geojson_bytes = geojson_str.encode("utf-8")
    print(f"writing to {path}")
    with fs.open(path, "wb") as writer:  # write out to public-facing GCS?
        with gzip.GzipFile(fileobj=writer, mode="w") as gz:
            gz.write(geojson_bytes)
    if cache_seconds != 3600:
        fs.setxattrs(path, fixed_key_metadata={"cache_control": f"public, max-age={cache_seconds}"})
    base64state = base64.urlsafe_b64encode(json.dumps(state).encode()).decode()
    spa_map_url = f"{site}?state={base64state}"
    return spa_map_url


def set_state_export(
    gdf,
    bucket: str = SPA_MAP_BUCKET,
    subfolder: str = "testing/",
    filename: str = "test2",
    map_type: Literal["speedmap", "speed_variation", "hqta_areas", "hqta_stops", "state_highway_network"] = None,
    map_title: str = "Map",
    cmap: branca.colormap.ColorMap = None,
    color_col: str = None,
    legend_url: str = None,
    existing_state: dict = {},
    cache_seconds: int = 3600,
    manual_centroid: list = None,
):
    """
    Applies light formatting to gdf for successful spa display. Will pass map_type
    if supported by the spa and provided. GCS bucket is preset to the publically
    available one.
    Supply cmap and color_col for coloring based on a Branca ColorMap and a column
    to apply the color to.
    Cache is 1 hour by default, can set shorter time in seconds for
    "near realtime" applications (suggest 120) or development (suggest 0)

    Returns dict with state dictionary and map URL. Can call multiple times and supply
    previous state as existing_state to create multilayered maps.
    """
    assert not gdf.empty, "geodataframe is empty!"
    spa_map_state = existing_state or {"name": "null", "layers": [], "lat_lon": (), "zoom": 13}
    path = f"{bucket}{subfolder}{filename}.geojson.gz"
    gdf = gdf.to_crs(geography_utils.WGS84)
    if cmap and color_col:
        gdf["color"] = gdf[color_col].apply(lambda x: cmap.rgb_bytes_tuple(x))
    gdf = gdf.round(2)  # round for map display
    this_layer = [
        {
            "name": f"{map_title}",
            "url": f"https://storage.googleapis.com/{path}",
            "properties": {"stroked": False, "highlight_saturation_multiplier": 0.5},
        }
    ]
    if map_type:
        this_layer[0]["type"] = map_type
    if map_type == "speedmap":
        this_layer[0]["properties"]["tooltip_speed_key"] = "p20_mph"
    spa_map_state["layers"] += this_layer
    if manual_centroid:
        centroid = manual_centroid
    else:
        centroid = (gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean())
    spa_map_state["lat_lon"] = centroid
    if legend_url:
        spa_map_state["legend_url"] = legend_url
    return {
        "state_dict": spa_map_state,
        "spa_link": spa_map_export_link(gdf=gdf, path=path, state=spa_map_state, cache_seconds=cache_seconds),
    }
