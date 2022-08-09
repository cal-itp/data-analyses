"""
GTFS utils.

Queries to grab trips, stops, routes.

TODO: move over some of the rt_utils
over here, if it addresses GTFS schedule data more generally,
such as cleaning/reformatting arrival times.

Leave the RT-specific analysis there.
"""
import datetime

import dask.dataframe as dd
import dask_bigquery
import geopandas as gpd
import pandas as pd
from calitp.tables import tbl
from shared_utils import geography_utils
from siuba import *

GCS_PROJECT = "cal-itp-data-infra"

# import os
# os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

YESTERDAY_DATE = datetime.date.today() + datetime.timedelta(days=-1)


METROLINK_SHAPE_TO_ROUTE = {
    "AVin": "Antelope Valley Line",
    "AVout": "Antelope Valley Line",
    "OCin": "Orange County Line",
    "OCout": "Orange County Line",
    "LAXin": "LAX FlyAway Bus",
    "LAXout": "LAX FlyAway Bus",
    "SBin": "San Bernardino Line",
    "SBout": "San Bernardino Line",
    "VTin": "Ventura County Line",
    "VTout": "Ventura County Line",
    "91in": "91 Line",
    "91out": "91 Line",
    "IEOCin": "Inland Emp.-Orange Co. Line",
    "IEOCout": "Inland Emp.-Orange Co. Line",
    "RIVERin": "Riverside Line",
    "RIVERout": "Riverside Line",
}

METROLINK_ROUTE_TO_SHAPE = dict((v, k) for k, v in METROLINK_SHAPE_TO_ROUTE.items())


def fill_in_metrolink_trips_df_with_shape_id(trips: pd.DataFrame) -> pd.DataFrame:
    """
    trips: pandas.DataFrame.
            What is returned from tbl.views.gtfs_schedule_dim_trips()
            or some dataframe derived from that.

    Returns only Metrolink rows, with shape_id filled in.
    """
    # Even if an entire routes df is supplied, subset to just Metrolink
    df = trips[trips.calitp_itp_id == 323].reset_index(drop=True)

    # direction_id==1 (inbound), toward LA Union Station
    # direction_id==0 (outbound), toward Irvine/Oceanside, etc

    df = df.assign(
        shape_id=df.route_id.apply(lambda x: METROLINK_ROUTE_TO_SHAPE[x])
        .str.replace("in", "")
        .str.replace("out", "")
    )

    # OCin and OCout are not distinguished in dictionary
    df = df.assign(
        shape_id=df.apply(
            lambda x: x.shape_id + "out"
            if x.direction_id == "0"
            else x.shape_id + "in",
            axis=1,
        )
    )

    return df


def get_route_info(
    selected_date: str | datetime.date = YESTERDAY_DATE,
    itp_id_list: list[int] = None,
    route_cols: list[str] = None,
    get_df: bool = True,
) -> pd.DataFrame:

    # Route info query
    dim_routes = tbl.views.gtfs_schedule_dim_routes() >> distinct()

    routes = (
        tbl.views.gtfs_schedule_fact_daily_feed_routes()
        >> filter(
            _.date == selected_date,
            _.calitp_extracted_at <= selected_date,
            _.calitp_deleted_at >= selected_date,
        )
        # Drop one set of these (extracted_at/deleted_at),
        # since adding it into the merge cols sometimes returns zero rows
        >> select(-_.calitp_extracted_at, -_.calitp_deleted_at)
        >> inner_join(_, dim_routes, on=["route_key"])
        >> distinct()
    )

    if itp_id_list is not None:
        routes = routes >> filter(_.calitp_itp_id.isin(itp_id_list))

    if route_cols is not None:
        routes = routes >> select(*route_cols)

    if get_df is True:
        routes = routes >> collect()

    return routes


def get_stops(
    selected_date: str | datetime.date = YESTERDAY_DATE,
    itp_id_list: list[int] = None,
    stop_cols: list[str] = None,
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
) -> gpd.GeoDataFrame:

    # Stops query
    dim_stops = tbl.views.gtfs_schedule_dim_stops() >> distinct()

    stops = (
        tbl.views.gtfs_schedule_fact_daily_feed_stops()
        >> filter(
            _.date == selected_date,
            _.calitp_extracted_at <= selected_date,
            _.calitp_deleted_at >= selected_date,
        )
        # Drop one set of these (extracted_at/deleted_at),
        # since adding it into the merge cols sometimes returns zero rows
        >> select(-_.calitp_extracted_at, -_.calitp_deleted_at)
        >> inner_join(_, dim_stops, on=["stop_key"])
        >> distinct()
    )

    if itp_id_list is not None:
        stops = stops >> filter(_.calitp_itp_id.isin(itp_id_list))

    if stop_cols is not None:
        stops = stops >> select(*stop_cols)

    if get_df is True:
        stops = stops >> collect()
        stops = geography_utils.create_point_geometry(stops, crs=crs).drop(
            columns=["stop_lon", "stop_lat"]
        )

    return stops


def get_trips(
    selected_date: str | datetime.date = YESTERDAY_DATE,
    itp_id_list: list[int] = None,
    trip_cols: list[str] = None,
    get_df: bool = True,
) -> pd.DataFrame:

    # Trips query
    dim_trips = tbl.views.gtfs_schedule_dim_trips() >> distinct()

    trips = (
        tbl.views.gtfs_schedule_fact_daily_trips()
        >> filter(
            _.service_date == selected_date,
            _.calitp_extracted_at <= selected_date,
            _.calitp_deleted_at >= selected_date,
            _.is_in_service == True,
        )
        # Drop one set of these (extracted_at/deleted_at),
        # since adding it into the merge cols sometimes returns zero rows
        >> select(-_.calitp_extracted_at, -_.calitp_deleted_at)
        >> inner_join(
            _,
            dim_trips,
            on=[
                "trip_key",
                "trip_id",
                "route_id",
                "service_id",
                "calitp_itp_id",
                "calitp_url_number",
            ],
        )
        >> distinct()
    )

    metrolink_trips = trips >> filter(_.calitp_itp_id == 323) >> collect()

    if itp_id_list is not None:
        trips = trips >> filter(_.calitp_itp_id.isin(itp_id_list))

    if trip_cols is not None:
        trips = trips >> select(*trip_cols)

    if get_df is True:
        trips = trips >> filter(_.calitp_itp_id != 323) >> collect()

        # Fix Metrolink trips as a pd.DataFrame, then concatenate
        # This means that LazyTbl output will not show correct results
        # If Metrolink is not in itp_id_list, then this is empty dataframe, and that's ok
        corrected_metrolink = fill_in_metrolink_trips_df_with_shape_id(metrolink_trips)

        if trip_cols is not None:
            corrected_metrolink = corrected_metrolink[trip_cols]

        trips = pd.concat(
            [trips, corrected_metrolink], axis=0, ignore_index=True
        ).reset_index(drop=True)

    return trips


def get_route_shapes(
    selected_date: str | datetime.date,
    itp_id_list: list[int] = None,
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
) -> gpd.GeoDataFrame:
    """
    Return a subset of geography_utils.make_routes_gdf()
    to only have the `shape_id` values present on a selected day.

    geography_utils.make_routes_gdf() only selects based on calitp_extracted_at
    and calitp_deleted_at date range.
    """
    trips = (
        get_trips(
            selected_date=selected_date,
            itp_id_list=itp_id_list,
            trip_cols=["calitp_itp_id", "calitp_url_number", "shape_id"],
            get_df=False,
        )
        >> distinct()
        >> collect()
    )

    route_shapes = (
        geography_utils.make_routes_gdf(
            SELECTED_DATE=selected_date, CRS=crs, ITP_ID_LIST=itp_id_list
        )
        .drop(columns=["pt_array"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    route_shapes_on_day = pd.merge(
        route_shapes,
        trips,
        on=["calitp_itp_id", "calitp_url_number", "shape_id"],
        how="inner",
    )

    return route_shapes_on_day


def fix_departure_time(stop_times: dd.DataFrame) -> dd.DataFrame:
    # Some fixing, transformation, aggregation with dask
    # Grab departure hour
    # https://stackoverflow.com/questions/45428292/how-to-convert-pandas-str-split-call-to-to-dask
    stop_times2 = stop_times[~stop_times.departure_time.isna()].reset_index(drop=True)

    ddf = stop_times2.assign(
        departure_hour=stop_times2.departure_time.str.partition(":")[0].astype(int)
    )

    # Since hours past 24 are allowed for overnight trips
    # coerce these to fall between 0-23
    # https://stackoverflow.com/questions/54955833/apply-a-lambda-function-to-a-dask-dataframe
    ddf["departure_hour"] = ddf.departure_hour.map(lambda x: x - 24 if x >= 24 else x)

    return ddf


def check_departure_hours_input(departure_hours: tuple | list) -> list:
    """
    If given a tuple(start_hour, end_hour), it will return a list of the values.
    Ex: (0, 4) will return [0, 1, 2, 3]

    Returns a list of departure hours
    """
    if isinstance(departure_hours, tuple):
        return list(range(departure_hours[0], departure_hours[1]))
    elif isinstance(departure_hours, list):
        return departure_hours


def get_stop_times(
    selected_date: str | datetime.date = YESTERDAY_DATE,
    itp_id_list: list[int] = None,
    departure_hours: list[int] | tuple[int] = None,
    stop_time_cols: list[str] = None,
    # get_df: bool = False
) -> dd.DataFrame:
    """
    Returns a stop_times table from `tbl.views.gtfs_schedule_dim_stop_times.`
    This table usually is huge, so let's get some filtering done in Dask.
    Allow additional data processing by returning a Dask DataFrame.

    get_df: bool.
            If True, returns pd.DataFrame.
            If False, returns dd.DataFrame.
            Default to False because stop_times is big, reading it in-memory
            will probably not work.
    """
    ddf = dask_bigquery.read_gbq(
        project_id=GCS_PROJECT,
        dataset_id="views",
        table_id="gtfs_schedule_dim_stop_times",
    )

    # Fix departure times (coerce any that pass the 24 hr mark into falling between 0-24)
    ddf = fix_departure_time(ddf)

    if itp_id_list is not None:
        ddf = ddf[ddf.calitp_itp_id.isin(itp_id_list)].reset_index(drop=True)

    if departure_hours is not None:
        # Convert departure hours into list for filtering
        departure_hours = check_departure_hours_input(departure_hours)

        ddf = ddf[ddf.departure_hour.isin(departure_hours)].reset_index(drop=True)

    if stop_time_cols is not None:
        ddf = ddf[stop_time_cols].reset_index(drop=True)

    return ddf
