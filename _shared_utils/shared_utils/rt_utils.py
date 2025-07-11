import datetime as dt
import os
import time
from pathlib import Path

import branca
import dask_geopandas as dg
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

MPH_PER_MPS = 2.237  # use to convert meters/second to miles/hour
METERS_PER_MILE = 1609.34

SPEEDMAP_LEGEND_URL = "https://storage.googleapis.com/calitp-map-tiles/speeds_legend.svg"
VARIANCE_LEGEND_URL = "https://storage.googleapis.com/calitp-map-tiles/variance_legend.svg"
ACCESS_SPEEDMAP_LEGEND_URL = "https://storage.googleapis.com/calitp-map-tiles/speeds_legend_color_access.svg"

ACCESS_ZERO_THIRTY_COLORSCALE = branca.colormap.step.RdBu_10.scale(vmin=0, vmax=30)
ACCESS_ZERO_THIRTY_COLORSCALE.caption = "Speed (miles per hour)"
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

# must include pt_geom to return gdf
shape_cols = ["feed_key", "shape_id"]

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
    gdf: gpd.GeoDataFrame | dg.GeoDataFrame,
) -> gpd.GeoDataFrame | dg.GeoDataFrame:
    """
    For a gdf, add the origin, destination columns given a linestring.
    Note: multilinestring may not work!
    https://gis.stackexchange.com/questions/358584/how-to-extract-long-and-lat-of-start-and-end-points-to-seperate-columns-from-t
    """
    if isinstance(gdf, dg.GeoDataFrame):
        gdf = gdf.assign(
            origin=gdf.geometry.apply(
                lambda x: shapely.get_point(x, 0),
                meta=("origin", "geometry"),
            ),
            destination=gdf.geometry.apply(
                lambda x: shapely.get_point(x, -1),
                meta=("destination", "geometry"),
            ),
        )

    elif isinstance(gdf, gpd.GeoDataFrame):
        gdf = gdf.assign(
            origin=gdf.geometry.apply(lambda x: shapely.get_point(x, 0)),
            destination=gdf.geometry.apply(lambda x: shapely.get_point(x, -1)),
        )

    return gdf


direction_grouping = {
    "Northbound": "north-south",
    "Southbound": "north-south",
    "Eastbound": "east-west",
    "Westbound": "east-west",
}


def add_route_cardinal_direction(
    df: gpd.GeoDataFrame | dg.GeoDataFrame,
    origin: str = "origin",
    destination: str = "destination",
) -> gpd.GeoDataFrame | dg.GeoDataFrame:
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


def check_cached(
    filename: str,
    GCS_FILE_PATH: str | Path = GCS_FILE_PATH,
    subfolder: str | Path = "cached_views/",
) -> str | Path:
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


def get_speedmaps_ix_df(analysis_date: dt.date, itp_id: int | None = None) -> pd.DataFrame:
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
            service_date, feed_key_list, crs=geography_utils.CA_NAD83Albers_m, shape_cols=shape_cols
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
    export_path: str | Path = EXPORT_PATH,
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
        r_pt2 = shapely.ops.substring(r_shift, r_shift.length, r_shift.length)
        l_shift = end_segment.parallel_offset(shift_distance, "left")
        l_pt = shapely.ops.substring(l_shift, 0, 0)
        l_pt2 = shapely.ops.substring(l_shift, l_shift.length, l_shift.length)
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


def describe_slowest(row):
    description = which_desc(row)
    full_description = (
        f"{row.route_short_name}{description}, {row.direction}: "
        f"{round(row.median_trip_mph, 1)} mph median trip speed for "
        f'{row.num_trips} trip{"s" if row.num_trips > 1 else ""}'
    )
    row["full_description"] = full_description
    return row
