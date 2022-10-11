import datetime as dt
import os
import re
import time
from pathlib import Path

import branca
import folium
import gcsfs
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from calitp import query_sql
from numba import jit
from shared_utils import geography_utils, gtfs_utils, map_utils, utils
from siuba import *

# from zoneinfo import ZoneInfo
# import warnings

fs = gcsfs.GCSFileSystem()

# set system time
os.environ["TZ"] = "America/Los_Angeles"
time.tzset()

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/rt_delay"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"
EXPORT_PATH = f"{GCS_FILE_PATH}cached_views/"

MPH_PER_MPS = 2.237  # use to convert meters/second to miles/hour

# Colorscale
ZERO_THIRTY_COLORSCALE = branca.colormap.step.RdYlGn_10.scale(vmin=0, vmax=30)
ZERO_THIRTY_COLORSCALE.caption = "Speed (miles per hour)"

# Datetime formats
DATE_WEEKDAY_FMT = "%b %d (%a)"  # Jun 01 (Wed) for 6/1/22
MONTH_DAY_FMT = "%m_%d"  # 6_01 for 6/1/22
HOUR_MIN_FMT = "%H:%M"  # 08:00 for 8 am, 13:00 for 1pm
HOUR_MIN_SEC_FMT = (
    "%H:%M:%S"  # 08:15:05 for 8:15 am + 5 sec, 13:15:05 for 1:15pm + 5 sec
)
FULL_DATE_FMT = "%Y-%m-%d"  # 2022-06-01 for 6/1/22


def convert_ts(ts: int) -> dt.datetime:
    pacific_dt = dt.datetime.fromtimestamp(ts)
    return pacific_dt


def reversed_colormap(existing: branca.colormap.ColorMap) -> branca.colormap.ColorMap:
    return branca.colormap.LinearColormap(
        colors=list(reversed(existing.colors)), vmin=existing.vmin, vmax=existing.vmax
    )


def primary_cardinal_direction(origin, destination) -> str:
    """
    origin: shapely.geometry.Point
    destination: shapely.geometry.Point

    Takes point geometry and returns the primary cardinal direction
    (north, south, east, west). To use, first grab the origin
    and destination of a line geometry.
    """
    distance_east = destination.x - origin.x
    distance_north = destination.y - origin.y

    if abs(distance_east) > abs(distance_north):
        if distance_east > 0:
            return "Eastbound"
        else:
            return "Westbound"
    else:
        if distance_north > 0:
            return "Northbound"
        else:
            return "Southbound"


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

    interpolator = lambda x: np.interp(x, xp, yp)
    df = df.assign(
        arrival_time=df.apply(
            lambda x: interpolator(x.shape_meters)
            if pd.isnull(x.arrival_time)
            else x.arrival_time,
            axis=1,
        )
    )

    df["arrival_time"] = df.arrival_time.to_numpy().astype("datetime64[s]")

    return df


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


def get_vehicle_positions(
    itp_id: int, analysis_date: dt.date, export_path: str | Path = EXPORT_PATH
) -> pd.DataFrame:
    """
    itp_id: an itp_id (string or integer)
    analysis_date: datetime.date

    Interim function for getting complete vehicle positions data for a
    single operator on a single date of interest.
    To be replaced as RT views are implemented...

    Currently drops positions for day after analysis date after 2AM,
    temporary fix to balance capturing trips crossing
    midnight with avoiding duplicates...
    """

    next_date = analysis_date + dt.timedelta(days=1)
    date_str = analysis_date.strftime(FULL_DATE_FMT)

    start = dt.datetime.combine(analysis_date, dt.time(0))
    end = start + dt.timedelta(days=1, seconds=2 * 60**2)

    filename = f"vp_{itp_id}_{date_str}.parquet"
    path = check_cached(filename)

    st_combined = dt.datetime.combine(analysis_date, dt.time(8))
    st_ts_utc = int(st_combined.timestamp())
    end_combined = dt.datetime.combine(
        analysis_date + dt.timedelta(days=1), dt.time(10)
    )
    end_ts_utc = int(end_combined.timestamp())

    if path:
        print("found parquet")
        return pd.read_parquet(path)
    else:
        df = query_sql(
            f"""
        SELECT calitp_itp_id, calitp_url_number,
        timestamp AS vehicle_timestamp,
        vehicle_label AS entity_id, vehicle_id,
        trip_id, longitude AS vehicle_longitude, latitude AS vehicle_latitude
        FROM `cal-itp-data-infra.staging.stg_rt__vehicle_positions`
        WHERE calitp_itp_id = {itp_id} AND date IN ("{analysis_date}", "{next_date}")
        AND timestamp > {st_ts_utc}
        AND timestamp < {end_ts_utc}
        """
        )

        df = df >> distinct(_.trip_id, _.vehicle_timestamp, _keep_all=True)
        df = df.dropna(subset=["vehicle_timestamp"])
        assert not df.empty, f"no vehicle positions data found for {date_str}"
        df.vehicle_timestamp = df.vehicle_timestamp.apply(convert_ts)
        # header timestamp not present in staging, add upstream if desired
        # df.header_timestamp = df.header_timestamp.apply(convert_ts)
        df = df >> filter(_.vehicle_timestamp > start, _.vehicle_timestamp < end)

        # assert df.vehicle_timestamp.min() < dt.datetime.combine(analysis_date, dt.time(0)), 'rt data starts after analysis date'
        # assert dt.datetime.combine(analysis_date, dt.time(hour=23, minute=59)) < df.vehicle_timestamp.max(), 'rt data ends early on analysis date'
        # if not df.vehicle_timestamp.min() < dt.datetime.combine(analysis_date, dt.time(0)):
        #     warnings.warn('rt data starts after analysis date')
        # if not dt.datetime.combine(end) < df.vehicle_timestamp.max():
        #     warnings.warn('rt data ends early on analysis date')

        df.to_parquet(f"{export_path}{filename}")
        return df


def get_routes(itp_id: int, analysis_date: dt.date):
    """
    Grab routes running for operator on selected date.

    Returns siuba.sql.verbs.LazyTbl.
    """
    keep_route_cols = [
        "calitp_itp_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_desc",
        "route_type",
    ]

    routes = gtfs_utils.get_route_info(
        selected_date=analysis_date,
        itp_id_list=[itp_id],
        route_cols=keep_route_cols,
        get_df=False,
    )

    return routes


def get_trips(
    itp_id: int,
    analysis_date: dt.date,
    force_clear: bool = False,
    route_types: list = None,
    export_path: str | Path = EXPORT_PATH,
) -> pd.DataFrame:
    """
    itp_id: an itp_id (string or integer)
    analysis_date: datetime.date
    route types: (optional) filter for certain GTFS route types

    Interim function for getting complete trips data for a single operator
    on a single date of interest.
    To be replaced as RT views are implemented...

    Updated to include route_short_name from routes
    """

    date_str = analysis_date.strftime(FULL_DATE_FMT)
    filename = f"trips_{itp_id}_{date_str}.parquet"

    path = check_cached(filename)

    if path and not force_clear:
        print("found parquet")
        cached = pd.read_parquet(path)
        if not cached.empty:
            trips = cached
        else:
            print("cached parquet empty, will try a fresh query")
            return get_trips(
                itp_id, analysis_date, force_clear=True, route_types=route_types
            )
    else:
        print("getting trips...")

        keep_trip_cols = [
            "calitp_itp_id",
            "calitp_url_number",
            "service_date",
            "trip_key",
            "trip_id",
            "route_id",
            "direction_id",
            "shape_id",
            "calitp_extracted_at",
            "calitp_deleted_at",
        ]

        trips = gtfs_utils.get_trips(
            selected_date=analysis_date,
            itp_id_list=[itp_id],
            trip_cols=keep_trip_cols,
            get_df=False,
        )

        # Grab the get_routes() function defined above
        # which already subsets to what we want, and returns a LazyTbl
        routes = get_routes(
            itp_id=itp_id,
            analysis_date=analysis_date,
        )

        # Keep both as LazyTbl to do inner join
        trips = (
            trips >> inner_join(_, routes, on=["calitp_itp_id", "route_id"])
        ) >> collect()

        # Drop duplicates (not able to drop when querying trips table
        # without forcing a collect()
        trips = trips.drop_duplicates(subset="trip_id").reset_index(drop=True)

        if not path or force_clear:
            trips.to_parquet(f"{export_path}{filename}")

    if route_types:
        print(f"filtering to GTFS route types {route_types}")
        trips = trips >> filter(_.route_type.isin(route_types))

    return trips


def get_stop_times(
    itp_id: int,
    analysis_date: dt.date,
    force_clear: bool = False,
    export_path: str | Path = EXPORT_PATH,
) -> pd.DataFrame:
    """
    itp_id: an itp_id (string or integer)
    analysis_date: datetime.date

    Interim function for getting complete stop times data for a single operator
    on a single date of interest.
    To be replaced as RT views are implemented...
    """
    date_str = analysis_date.strftime(FULL_DATE_FMT)
    filename = f"st_{itp_id}_{date_str}.parquet"

    path = check_cached(filename)

    if path and not force_clear:
        print("found parquet")
        cached = pd.read_parquet(path)
        if not cached.empty:
            return cached
        else:
            print("cached parquet empty, will try a fresh query")

    trip_df_setting = trips_cached(itp_id, date_str)

    st = gtfs_utils.get_stop_times(
        selected_date=analysis_date,
        itp_id_list=[itp_id],
        stop_time_cols=None,
        get_df=True,  # return pd.DataFrame in the end
        trip_df=trip_df_setting,
        departure_hours=None,  # no filtering, return all departure hours
    )

    st.to_parquet(f"{export_path}{filename}")

    return st


def get_stops(
    itp_id: int,
    analysis_date: dt.date,
    force_clear: bool = False,
    export_path: str | Path = EXPORT_PATH,
) -> gpd.GeoDataFrame:
    """
    itp_id: an itp_id (string or integer)
    analysis_date: datetime.date

    Interim function for getting complete stops data for a single operator on a single date of interest.
    To be replaced as RT views are implemented...
    """
    date_str = analysis_date.strftime(FULL_DATE_FMT)
    filename = f"stops_{itp_id}_{date_str}.parquet"

    path = check_cached(filename)

    if path and not force_clear:
        print("found parquet")
        cached = gpd.read_parquet(path)
        if not cached.empty:
            return cached
        else:
            print("cached parquet empty, will try a fresh query")

    keep_stop_cols = [
        "calitp_itp_id",
        "stop_id",
        "stop_lat",
        "stop_lon",
        "stop_name",
        "stop_key",
    ]

    stops = gtfs_utils.get_stops(
        selected_date=analysis_date,
        itp_id_list=[itp_id],
        stop_cols=keep_stop_cols,
        get_df=True,
        crs=geography_utils.CA_NAD83Albers,
    )

    utils.geoparquet_gcs_export(stops, export_path, filename)

    return stops


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
            print("cached parquet empty, will try a fresh query")
    else:

        trip_df_setting = trips_cached(itp_id, date_str)

        routelines = gtfs_utils.get_route_shapes(
            selected_date=analysis_date,
            itp_id_list=[itp_id],
            get_df=True,
            crs=geography_utils.CA_NAD83Albers,
            trip_df=trip_df_setting,
        )

        utils.geoparquet_gcs_export(routelines, export_path, filename)

        return routelines


def categorize_time_of_day(value: int | dt.datetime) -> str:
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


def arrowize_segment(
    line_geometry, arrow_distance: int = 15, buffer_distance: int = 20
):
    """Given a linestring segment from a gtfs shape,
    buffer and clip to show direction of progression"""

    try:
        # segment = line_geometry.parallel_offset(25, 'right')
        segment = line_geometry.simplify(tolerance=5)
        if segment.length < 50:  # return short segments unmodified, for now
            return segment.buffer(buffer_distance)
        arrow_distance = max(
            arrow_distance, line_geometry.length / 20
        )  # test this out?
        shift_distance = buffer_distance + 1

        begin_segment = shapely.ops.substring(
            segment, segment.length - 50, segment.length
        )
        r_shift = begin_segment.parallel_offset(shift_distance, "right")
        r_pt = shapely.ops.substring(r_shift, 0, 0)
        l_shift = begin_segment.parallel_offset(shift_distance, "left")
        l_pt = shapely.ops.substring(l_shift, l_shift.length, l_shift.length)
        end = shapely.ops.substring(
            begin_segment,
            begin_segment.length - arrow_distance,
            begin_segment.length - arrow_distance,
        )
        poly = shapely.geometry.Polygon(
            (r_pt, end, l_pt)
        )  # triangle to cut bottom of arrow
        # ends to the left
        end_segment = shapely.ops.substring(segment, 0, 50)
        end = shapely.ops.substring(end_segment, 0, 0)  # correct
        r_shift = end_segment.parallel_offset(shift_distance, "right")
        r_pt = shapely.ops.substring(r_shift, r_shift.length, r_shift.length)
        r_pt2 = shapely.ops.substring(
            r_shift, r_shift.length - arrow_distance, r_shift.length - arrow_distance
        )
        l_shift = end_segment.parallel_offset(shift_distance, "left")
        l_pt = shapely.ops.substring(l_shift, 0, 0)
        l_pt2 = shapely.ops.substring(l_shift, arrow_distance, arrow_distance)
        t1 = shapely.geometry.Polygon(
            (l_pt2, end, l_pt)
        )  # triangles to cut top of arrow
        t2 = shapely.geometry.Polygon((r_pt2, end, r_pt))
        segment_clip_mask = shapely.geometry.MultiPolygon((poly, t1, t2))
        # return segment_clip_mask

        differences = segment.buffer(buffer_distance).difference(segment_clip_mask)
        areas = [x.area for x in differences.geoms]
        for geom in differences.geoms:
            if geom.area == max(areas):
                return geom
    except Exception:
        return line_geometry.simplify(tolerance=5).buffer(buffer_distance)


def layer_points(rt_interpolator):
    keep_cols = [
        "geometry",
        "shape_meters",
        "progressed",
        "secs_from_last",
        "meters_from_last",
    ]

    initial_bk_noise = (
        rt_interpolator.position_gdf
        >> filter(_.meters_from_last < 0)
        >> select(*keep_cols)
    )

    initial_deduped = (
        rt_interpolator.position_gdf
        >> distinct(_.shape_meters, _keep_all=True)
        >> select(*keep_cols)
    )

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
    return map_utils.make_folium_multiple_layers_map(layers_dict, 900, 500)


def map_line(gdf):
    # gdf = gdf.buffer(1)
    gdf = gdf.to_crs(geography_utils.WGS84)
    centroid = gdf.geometry.iloc[0].centroid
    m = folium.Map(
        location=[centroid.y, centroid.x], zoom_start=13, tiles="cartodbpositron"
    )

    folium.GeoJson(gdf.to_json()).add_to(m)

    return m


def categorize_cleaning(rt_operator_day, interpolator_key):
    rt_interpolator = rt_operator_day.position_interpolators[interpolator_key]["rt"]
    raw = rt_interpolator.position_gdf.shape[0]
    same_loc_dropped = (rt_interpolator.position_gdf >> distinct(_.shape_meters)).shape[
        0
    ]
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
        f"{round(row.mean_delay_seconds/60, 0)} minutes late on average"
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

def get_operators(analysis_date, operator_list, generate_new=False):
    """
    Function for checking the existence of rt_trips and stop_delay_views in GCS for operators on a given day. 
    
    analysis_date: datetime.date
    operator_list: list of itp_id's 
    generate_new: 'True' to generate OperatorDayAnalysis and export to GCS, 'False' to not generate
    """
    fs_list = fs.ls(f'{shared_utils.rt_utils.GCS_FILE_PATH}rt_trips/')
    day = str(analysis_date.day).zfill(2)
    month = str(analysis_date.month).zfill(2)
    ## now finds ran operators on specific analysis date
    ran_operators = [int(path.split('rt_trips/')[1].split('_')[0])
                     for path in fs_list
                     if path.split('rt_trips/')[1] and path.split('rt_trips/')[1].split('_')[1] == month and path.split('rt_trips/')[1].split('_')[2][:2] == day]
    
    op_list_runstatus = {}
    for itp_id in operator_list:
        if itp_id in ran_operators:
            print(f'already ran: {itp_id}')
            op_list_runstatus[itp_id] = 'already_ran'
            continue
        else:
            if not generate_new:
                print (f'not yet run: {itp_id}')
                op_list_runstatus[itp_id] = 'not_yet_run'
            elif generate_new:
                print(f'calculating for agency: {itp_id}...')
                try:
                    rt_day = rt.OperatorDayAnalysis(itp_id, analysis_date)
                    rt_day.export_views_gcs()
                    print(f'complete for agency: {itp_id}')
                    op_list_runstatus[itp_id] = 'newly_run'
                except Exception as e:
                    print(f'rt failed for agency {itp_id}')
                    op_list_runstatus[itp_id] = 'new_failed_run'
                    print(e)
    return op_list_runstatus 