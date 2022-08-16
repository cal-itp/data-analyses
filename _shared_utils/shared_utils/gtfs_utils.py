"""
GTFS utils.

Queries to grab trips, stops, routes.
"""
import datetime
from typing import Literal

import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd
import siuba  # need this to do type hint in functions
from calitp.tables import tbl
from shared_utils import geography_utils, rt_utils, utils
from siuba import *

GCS_PROJECT = "cal-itp-data-infra"

# import os
# os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

YESTERDAY_DATE = datetime.date.today() + datetime.timedelta(days=-1)


# ----------------------------------------------------------------#
# Metrolink known error (shape_ids are missing in trips table).
# Fill it in manually.
# ----------------------------------------------------------------#
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


# ----------------------------------------------------------------#
# Convenience siuba filtering functions for querying
# ----------------------------------------------------------------#
def filter_itp_id(itp_id_list: list) -> siuba.dply.verbs.Pipeable:
    """
    Filter if itp_id_list is present.
    Otherwise, skip.
    """
    if itp_id_list is not None:
        return filter(_.calitp_itp_id.isin(itp_id_list))
    elif itp_id_list is None:
        return filter()


def subset_cols(cols: list) -> siuba.dply.verbs.Pipeable:
    """
    Select subset of columns, if column list is present.
    Otherwise, skip.
    """
    if cols is not None:
        return select(*cols)
    elif cols is None:
        # Can't use select(), because we'll select no columns
        # But, without knowing full list of columns, let's just
        # filter out nothing
        return filter()


def filter_custom_col(filter_dict: dict) -> siuba.dply.verbs.Pipeable:
    """
    Unpack the dictionary of custom columns / value to filter on.
    Key: column name
    Value: list with values to keep

    Otherwise, skip.

    TODO: Expand beyond 3. Does piping mean that order of conditions matter?

    Placement/order for where this filter happens...for stop_times..is it too late in the query?
    Should a check be included to run the filter if it finds the column in the first query it can?
    """
    if (filter_dict != {}) and (filter_dict is not None):

        keys, values = zip(*filter_dict.items())

        # Accommodate 3 filtering conditions for now
        if len(keys) >= 1:
            filter1 = filter(_[keys[0]].isin(values[0]))
            return filter1

        elif len(keys) >= 2:
            filter2 = filter(_[keys[1]].isin(values[1]))
            return filter1 >> filter2

        elif len(keys) >= 3:
            filter3 = filter(_[keys[2]].isin(values[2]))
            return filter1 >> filter2 >> filter3

    elif (filter_dict == {}) or (filter_dict is None):
        return filter()


# ----------------------------------------------------------------#
# Routes
# ----------------------------------------------------------------#
# Route Info - views.gtfs_schedule_dim_routes + views.gtfs_schedule_fact_daily_feed_routes
# Route Shapes - geography_utils.make_routes_gdf() + trips query
# Combine these 2?


def get_route_info(
    selected_date: str | datetime.date = YESTERDAY_DATE,
    itp_id_list: list[int] = None,
    route_cols: list[str] = None,
    get_df: bool = True,
    custom_filtering: dict = None,
) -> pd.DataFrame | siuba.sql.verbs.LazyTbl:

    # Route info query
    dim_routes = (
        tbl.views.gtfs_schedule_dim_routes() >> filter_itp_id(itp_id_list) >> distinct()
    )

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
        >> filter_custom_col(custom_filtering)
        >> subset_cols(route_cols)
        >> distinct()
    )

    if get_df is True:
        routes = routes >> collect()

    return routes


def get_route_shapes(
    selected_date: str | datetime.date,
    itp_id_list: list[int] = None,
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
    trip_df: siuba.sql.verbs.LazyTbl | pd.DataFrame = None,
    custom_filtering: dict = None,
) -> gpd.GeoDataFrame:
    """
    Return a subset of geography_utils.make_routes_gdf()
    to only have the `shape_id` values present on a selected day.

    geography_utils.make_routes_gdf() only selects based on calitp_extracted_at
    and calitp_deleted_at date range.

    Allow a pre-existing trips table to be supplied.
    If not, run a fresh trips query.

    Custom_filtering doesn't filter in the query (which relies on trips query),
    but can filter out after it's a gpd.GeoDataFrame
    """
    if trip_df is None:
        trips = (
            get_trips(
                selected_date=selected_date,
                itp_id_list=itp_id_list,
                trip_cols=["calitp_itp_id", "calitp_url_number", "shape_id"],
                get_df=True,
            )
            >> distinct()
        )
    # When a pre-existing table is given, convert it to pd.DataFrame
    # even if a LazyTbl is given
    elif trip_df is not None:
        if isinstance(trip_df, siuba.sql.verbs.LazyTbl):
            trips = trip_df >> collect()
        elif isinstance(trip_df, pd.DataFrame):
            trips = trip_df

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

    route_shapes_on_day = route_shapes_on_day >> filter_custom_col(custom_filtering)

    return route_shapes_on_day


# ----------------------------------------------------------------#
# Stops
# ----------------------------------------------------------------#
# views.gtfs_schedule_dim_stops + views.gtfs_schedule_fact_daily_feed_stops
def get_stops(
    selected_date: str | datetime.date = YESTERDAY_DATE,
    itp_id_list: list[int] = None,
    stop_cols: list[str] = None,
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
    custom_filtering: dict = None,
) -> gpd.GeoDataFrame | siuba.sql.verbs.LazyTbl:

    # Stops query
    dim_stops = (
        tbl.views.gtfs_schedule_dim_stops() >> filter_itp_id(itp_id_list) >> distinct()
    )

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
        >> filter_custom_col(custom_filtering)
        >> subset_cols(stop_cols)
        >> distinct()
    )

    if get_df is True:
        stops = stops >> collect()
        stops = geography_utils.create_point_geometry(stops, crs=crs).drop(
            columns=["stop_lon", "stop_lat"]
        )

    return stops


# ----------------------------------------------------------------#
# Trips
# ----------------------------------------------------------------#
# views.gtfs_schedule_dim_trips + views.gtfs_schedule_fact_daily_trips + handle metrolink
def get_trips(
    selected_date: str | datetime.date = YESTERDAY_DATE,
    itp_id_list: list[int] = None,
    trip_cols: list[str] = None,
    get_df: bool = True,
    custom_filtering: dict = None,
) -> pd.DataFrame | siuba.sql.verbs.LazyTbl:

    # Trips query
    dim_trips = (
        tbl.views.gtfs_schedule_dim_trips() >> filter_itp_id(itp_id_list) >> distinct()
    )

    trips = (
        tbl.views.gtfs_schedule_fact_daily_trips()
        >> filter(
            _.service_date == selected_date,
            _.calitp_extracted_at <= selected_date,
            _.calitp_deleted_at >= selected_date,
            _.is_in_service == True,
        )
        >> filter_itp_id(itp_id_list)
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
        >> filter_custom_col(custom_filtering)
        >> subset_cols(trip_cols)
        >> distinct()
    )

    # Handle Metrolink when we need to
    if ((itp_id_list is None) or (323 in itp_id_list)) and (get_df is True):
        metrolink_trips = trips >> filter(_.calitp_itp_id == 323) >> collect()
        not_metrolink_trips = trips >> filter(_.calitp_itp_id != 323) >> collect()

        # Fix Metrolink trips as a pd.DataFrame, then concatenate
        # This means that LazyTbl output will not show correct results
        # If Metrolink is not in itp_id_list, then this is empty dataframe, and that's ok
        corrected_metrolink = fill_in_metrolink_trips_df_with_shape_id(metrolink_trips)

        trips = pd.concat(
            [not_metrolink_trips, corrected_metrolink], axis=0, ignore_index=True
        ).reset_index(drop=True)

    elif (itp_id_list is not None) and (323 not in itp_id_list) and (get_df is True):
        trips = trips >> collect()

    return trips


# ----------------------------------------------------------------#
# Stop Times
# ----------------------------------------------------------------#
# views.gtfs_schedule_dim_stop_times + views.gtfs_schedule_index_feed_trip_stops + trips query
def fix_departure_time(stop_times: dd.DataFrame) -> dd.DataFrame:
    """
    Some fixing, transformation, aggregation with dask
    Add departure hour column
    https://stackoverflow.com/questions/45428292/how-to-convert-pandas-str-split-call-to-to-dask
    """
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
    stop_time_cols: list[str] = None,
    get_df: bool = False,
    departure_hours: tuple | list = None,
    trip_df: pd.DataFrame | siuba.sql.verbs.LazyTbl = None,
    custom_filtering: dict = None,
) -> dd.DataFrame | pd.DataFrame:
    """
    Download stop times table for operator on a day.

    Since it's huge, return dask dataframe by default, to
    allow for more wrangling before turning it back to pd.DataFrame.

    Allow a pre-existing trips table to be supplied.
    If not, run a fresh trips query.

    get_df: bool.
            If True, return pd.DataFrame
            If False, return dd.DataFrame

    TODO: custom_filtering...is it placed too late in the query?
    """
    if trip_df is None:
        # Grab the trips for that day
        trips_on_day = (
            get_trips(
                selected_date=selected_date,
                itp_id_list=itp_id_list,
                trip_cols=["trip_key"],
                get_df=False,
            )
            >> distinct()
        )

    elif trip_df is not None:
        keep_cols = ["trip_key"]
        if isinstance(trip_df, siuba.sql.verbs.LazyTbl):
            trips_on_day = trip_df >> select(keep_cols)
        # Have to handle pd.DataFrame separately later
        elif isinstance(trip_df, pd.DataFrame):
            trips_on_day = trip_df[keep_cols]

    stop_times_query = (
        tbl.views.gtfs_schedule_dim_stop_times()
        >> filter_itp_id(itp_id_list)
        >> select(-_.calitp_url_number)
        >> distinct()
    )

    # Use the gtfs_schedule_index_feed_trip_stops to find the stop_time_keys
    # that actually occurred on that day
    # Handle the pd.DataFrame trips_on_day table separately
    if isinstance(trips_on_day, pd.DataFrame):
        keep_trip_keys = pd.Series(trips_on_day.trip_key)

        # This table only has keys, no itp_id or date to filter on
        trips_stops_ix_query = (
            tbl.views.gtfs_schedule_index_feed_trip_stops()
            >> select(_.trip_key, _.stop_time_key)
            >> distinct()
            # When it's pd.DataFrame, let's use filtering against a pd.Series to accomplish this
            # since inner_join will not work unless it's LzyTbl on both sides
            >> filter(_.trip_key.isin(keep_trip_keys))
        )

    else:
        trips_stops_ix_query = (
            tbl.views.gtfs_schedule_index_feed_trip_stops()
            >> select(_.trip_key, _.stop_time_key)
            >> distinct()
        ) >> inner_join(_, trips_on_day, on="trip_key")

    stop_times = (
        stop_times_query
        >> inner_join(_, trips_stops_ix_query, on="stop_time_key")
        >> mutate(stop_sequence=_.stop_sequence.astype(int))  # in SQL!
        >> filter_custom_col(custom_filtering)
        >> collect()
        >> distinct(_.stop_id, _.trip_id, _keep_all=True)
        >> arrange(_.trip_id, _.stop_sequence)
    )

    # Turn to dask dataframe
    stop_times_ddf = dd.from_pandas(stop_times, npartitions=1)

    # Basic cleaning of arrival / departure time and dropping duplicates
    stop_times_ddf = (
        stop_times_ddf.assign(
            arrival_time=stop_times.arrival_time.str.strip(),
            departure_time=stop_times.departure_time.str.strip(),
        )
        .drop_duplicates(subset=["stop_id", "trip_id"])
        .reset_index(drop=True)
    )

    # Correctly parse departure times if they cross 24 hr threshold
    stop_times_cleaned = fix_departure_time(stop_times_ddf)

    if departure_hours is not None:
        # If tuple is given, change that to a list of departure hours
        departure_hours_cleaned = check_departure_hours_input(departure_hours)

        # Subset by departure hours
        stop_times_cleaned = stop_times_cleaned[
            stop_times_cleaned.departure_hour.isin(departure_hours_cleaned)
        ].reset_index(drop=True)

    # Subset at the very end, otherwise cleaning of departure times won't work
    if stop_time_cols is not None:
        stop_times_cleaned = stop_times_cleaned[stop_time_cols]

    if get_df is True:
        stop_times_cleaned = stop_times_cleaned.compute()

    return stop_times_cleaned


# ----------------------------------------------------------------#
# Concatenate all stashed files in GCS
# ----------------------------------------------------------------#
# Moving forward, should use cached files whenever possible
# especially for external-facing analysis
# get the most use out of caching and create the same
# "universe" of operators whenever we can


def format_date(analysis_date: datetime.date | str) -> str:
    """
    Get date formatted correctly in all the queries
    """
    if isinstance(analysis_date, datetime.date):
        return analysis_date.strftime(rt_utils.FULL_DATE_FMT)
    elif isinstance(analysis_date, str):
        return datetime.datetime.strptime(analysis_date, rt_utils.FULL_DATE_FMT).date()


ALL_ITP_IDS = (
    tbl.gtfs_schedule.agency() >> select(_.calitp_itp_id) >> distinct() >> collect()
).calitp_itp_id.tolist()


def all_routelines_or_stops_with_cached(
    dataset: Literal["routelines", "stops"] = "routelines",
    analysis_date: datetime.date | str = "2022-06-15",
    itp_id_list: list = ALL_ITP_IDS,
    export_path="gs://calitp-analytics-data/data-analyses/traffic_ops/",
):
    """
    Use cached files whenever possible, instead of running fresh query.
    Routelines and stops are geospatial, import and export with geopandas.
    """

    date_str = format_date(analysis_date)

    # Set metadata
    gdf = dg.read_parquet(
        f"{rt_utils.EXPORT_PATH}{dataset}_182_{date_str}.parquet"
    ).head(0)

    for itp_id in sorted(itp_id_list):

        filename = f"{dataset}_{itp_id}_{date_str}.parquet"
        path = rt_utils.check_cached(filename)

        if path:
            operator = dg.read_parquet(path)
            gdf = dd.multi.concat([gdf, operator], axis=0, ignore_index=True)

    gdf = gdf.compute().reset_index(drop=True)

    utils.geoparquet_gcs_export(gdf, export_path, f"{dataset}_{date_str}")


def all_trips_or_stoptimes_with_cached(
    dataset: Literal["trips", "st"] = "trips",
    analysis_date: datetime.date | str = "2022-06-15",
    itp_id_list: list = ALL_ITP_IDS,
    export_path="gs://calitp-analytics-data/data-analyses/traffic_ops/",
):
    """
    Use cached files whenever possible, instead of running fresh query.
    Trips and stop times are tabular, import and export with pandas.

    Unlikely that stop times will ever need to be concatenated?
    """
    date_str = format_date(analysis_date)

    # Set metadata
    df = pd.read_parquet(
        f"{rt_utils.EXPORT_PATH}{dataset}_182_{date_str}.parquet"
    ).head(0)
    df = dd.from_pandas(df, npartitions=1)

    for itp_id in sorted(itp_id_list):

        filename = f"{dataset}_{itp_id}_{date_str}.parquet"
        path = rt_utils.check_cached(filename)

        if path:
            operator = pd.read_parquet(path)
            df = dd.multi.concat([df, operator], axis=0, ignore_index=True)

    df = df.compute().reset_index(drop=True)

    df.to_parquet(f"{export_path}{dataset}_{date_str}.parquet")
