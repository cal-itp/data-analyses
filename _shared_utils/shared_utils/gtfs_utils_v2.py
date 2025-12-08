"""
GTFS utils for v2 warehouse
"""

import datetime
import os
from typing import Literal, Union

import geopandas as gpd
import pandas as pd
import shapely
import siuba  # need this to do type hint in functions
import sqlalchemy
from calitp_data_analysis import geography_utils
from calitp_data_analysis.sql import CALITP_BQ_LOCATION, CALITP_BQ_MAX_BYTES
from calitp_data_analysis.tables import AutoTable
from shared_utils import DBSession, schedule_rt_utils
from shared_utils.models.dim_gtfs_dataset import DimGtfsDataset
from shared_utils.models.dim_stop_time import DimStopTime
from shared_utils.models.fct_daily_schedule_feeds import FctDailyScheduleFeeds
from shared_utils.models.fct_daily_scheduled_shapes import FctDailyScheduledShapes
from shared_utils.models.fct_daily_scheduled_stops import FctDailyScheduledStops
from shared_utils.models.fct_scheduled_trips import FctScheduledTrips
from siuba import *
from sqlalchemy import and_, create_engine, func, or_, select

ROUTE_TYPE_DICT = {
    # https://gtfs.org/documentation/schedule/reference/#routestxt
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

RAIL_ROUTE_TYPES = [k for k, v in ROUTE_TYPE_DICT.items() if k not in ["3", "4"]]


def _get_engine(max_bytes=None, project="cal-itp-data-infra"):
    max_bytes = CALITP_BQ_MAX_BYTES if max_bytes is None else max_bytes

    cred_path = os.environ.get("CALITP_SERVICE_KEY_PATH")

    # Note that we should be able to add location as a uri parameter, but
    # it is not being picked up, so passing as a separate argument for now.

    engine = create_engine(
        f"bigquery://{project}/?maximum_bytes_billed={max_bytes}",  # noqa: E231
        location=CALITP_BQ_LOCATION,
        credentials_path=cred_path,
    )

    return engine


def _get_tables():
    tables = AutoTable(
        _get_engine(project="cal-itp-data-infra-staging"),
        lambda s: s,  # s.replace(".", "_"),
    )

    tables._init()

    return tables


# ----------------------------------------------------------------#
# Convenience siuba filtering functions for querying
# ----------------------------------------------------------------#


def filter_operator(operator_feeds: list, include_name: bool = False) -> siuba.dply.verbs.Pipeable:
    """
    Filter if operator_list is present.
    For trips table, operator_feeds can be a list of names or feed_keys.
    For stops, shapes, stop_times, operator_feeds can only be a list of feed_keys.
    """
    # in testing, using _.feed_key or _.name came up with a
    # siuba verb not implemented
    # https://github.com/machow/siuba/issues/407
    # put brackets around should work
    if include_name:
        return filter(_["feed_key"].isin(operator_feeds) | _["name"].isin(operator_feeds))
    else:
        return filter(_["feed_key"].isin(operator_feeds))


def filter_date(
    selected_date: Union[str, datetime.date], date_col: Literal["service_date", "activity_date"]
) -> siuba.dply.verbs.Pipeable:
    return filter(_[date_col] == selected_date)


def subset_cols(cols: list) -> siuba.dply.verbs.Pipeable:
    """
    Select subset of columns, if column list is present.
    Otherwise, skip.
    """
    if cols:
        return select(*cols)
    elif not cols or len(cols) == 0:
        # Can't use select(), because we'll select no columns
        # But, without knowing full list of columns, let's just
        # filter out nothing
        return filter()


def filter_custom_col(filter_dict: dict) -> siuba.dply.verbs.Pipeable:
    """
    Unpack the dictionary of custom columns / value to filter on.
    Will unpack up to 5 other conditions...since we now have
    a larger fct_daily_trips table.

    Key: column name
    Value: list with values to keep

    Otherwise, skip.
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

        elif len(keys) >= 4:
            filter4 = filter(_[keys[3]].isin(values[3]))
            return filter1 >> filter2 >> filter3 >> filter4

        elif len(keys) >= 5:
            filter5 = filter(_[keys[4]].isin(values[4]))
            return filter1 >> filter2 >> filter3 >> filter4 >> filter5

    elif (filter_dict == {}) or (filter_dict is None):
        return filter()


def check_operator_feeds(operator_feeds: list[str]):
    if len(operator_feeds) == 0:
        raise ValueError("Supply list of feed keys or operator names!")


# ----------------------------------------------------------------#
# Metrolink fixes (shape_ids are missing in trips table).
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


def get_metrolink_feed_key(
    selected_date: Union[str, datetime.date], get_df: bool = False, **kwargs
) -> Union[pd.DataFrame, str]:
    """
    Get Metrolink's feed_key value.
    """
    metrolink_in_airtable = schedule_rt_utils.filter_dim_gtfs_datasets(
        keep_cols=["key", "name"],
        custom_filtering={"name": ["Metrolink Schedule"]},
        get_df=False,
    )

    statement = (
        metrolink_in_airtable.add_columns(FctDailyScheduleFeeds.feed_key)
        .join(
            FctDailyScheduleFeeds,
            and_(
                FctDailyScheduleFeeds.gtfs_dataset_key == DimGtfsDataset.key,
                FctDailyScheduleFeeds.gtfs_dataset_name == DimGtfsDataset.name,
            ),
        )
        .where(FctDailyScheduleFeeds.date == selected_date)
    )

    with DBSession() as session:
        metrolink_feed = pd.read_sql(statement, session.bind)

    metrolink_feed = metrolink_feed.rename(columns={"gtfs_dataset_name": "name"})[["feed_key", "name"]]

    if get_df:
        return metrolink_feed
    else:
        return metrolink_feed.feed_key.iloc[0]


def fill_in_metrolink_trips_df_with_shape_id(trips: pd.DataFrame, metrolink_feed_key: str) -> pd.DataFrame:
    """
    trips: pandas.DataFrame.
            What is returned from tbls.mart_gtfs.fct_daily_scheduled_trips
            or some dataframe derived from that.

    Returns only Metrolink rows, with shape_id filled in.
    """
    # Even if an entire routes df is supplied, subset to just Metrolink
    df = trips[trips.feed_key == metrolink_feed_key].reset_index(drop=True)

    # direction_id==1 (inbound), toward LA Union Station
    # direction_id==0 (outbound), toward Irvine/Oceanside, etc

    df = df.assign(
        shape_id=df.route_id.apply(lambda x: METROLINK_ROUTE_TO_SHAPE[x]).str.replace("in", "").str.replace("out", "")
    )

    # OCin and OCout are not distinguished in dictionary
    df = df.assign(
        shape_id=df.apply(
            lambda x: x.shape_id + "out" if x.direction_id == 0 else x.shape_id + "in",
            axis=1,
        )
    )

    return df


def schedule_daily_feed_to_gtfs_dataset_name(
    selected_date: Union[str, datetime.date],
    keep_cols: list[str] = [],
    get_df: bool = True,
    feed_option: Literal[
        "customer_facing",
        "use_subfeeds",
        "current_feeds",
        "include_precursor",
    ] = "use_subfeeds",
) -> Union[pd.DataFrame, sqlalchemy.sql.selectable.Select]:
    """
    Select a date, find what feeds are present, and
    merge in organization name

    Analyst would manually include or exclude feeds based on any of the columns.
    Custom filtering doesn't work well, with all the NaNs/Nones/booleans.

    As we move down the options, generally, more rows should be returned.

    * customer_facing: when there are multiple feeds for an organization,
            favor the customer facing one.
            Applies to: Bay Area 511 combined feed favored over subfeeds

    * use_subfeeds: when there are multiple feeds for an organization,
            favor the subfeeds.
            Applies to: Bay Area 511 subfeeds favored over combind feed

    * current_feeds: all current feeds (combined and subfeeds present)

    * include_precursor: include precursor feeds
                Caution: would result in duplicate organization names
    """
    # Get GTFS schedule datasets from Airtable
    dim_gtfs_datasets = schedule_rt_utils.filter_dim_gtfs_datasets(
        keep_cols=["key", "name", "type", "regional_feed_type"],
        custom_filtering={"type": ["schedule"]},
        get_df=False,
    )

    is_not_regional_precursor = func.IFNULL(DimGtfsDataset.regional_feed_type, "") != "Regional Precursor Feed"
    additional_search_conditions = {
        "customer_facing": [
            is_not_regional_precursor,
            func.IFNULL(DimGtfsDataset.regional_feed_type, "") != "Regional Subfeed",
        ],
        "use_subfeeds": [
            is_not_regional_precursor,
            # keep VCTC combined because the combined feed is the only feed
            func.IFNULL(DimGtfsDataset.name, "") != "Bay Area 511 Regional Schedule",
        ],
        "current_feeds": [is_not_regional_precursor],
    }

    search_conditions = [FctDailyScheduleFeeds.date == selected_date] + additional_search_conditions.get(
        feed_option, []
    )

    # Join on gtfs_dataset_key to get organization name
    statement = (
        dim_gtfs_datasets.with_only_columns(
            DimGtfsDataset.regional_feed_type,
            DimGtfsDataset.type,
            FctDailyScheduleFeeds.key,
            FctDailyScheduleFeeds.date,
            FctDailyScheduleFeeds.feed_key,
            FctDailyScheduleFeeds.feed_timezone,
            FctDailyScheduleFeeds.base64_url,
            FctDailyScheduleFeeds.gtfs_dataset_key,
            FctDailyScheduleFeeds.gtfs_dataset_name.label("name"),
        )
        .join(
            FctDailyScheduleFeeds,
            and_(
                FctDailyScheduleFeeds.gtfs_dataset_key == DimGtfsDataset.key,
                FctDailyScheduleFeeds.gtfs_dataset_name == DimGtfsDataset.name,
            ),
        )
        .where(and_(*search_conditions))
    )

    if keep_cols and len(keep_cols):
        columns = []
        for column in keep_cols:
            new_column = DimGtfsDataset.name if column == "name" else getattr(FctDailyScheduleFeeds, column)
            columns.append(new_column)
        statement = statement.with_only_columns(columns)

    if get_df:
        with DBSession() as session:
            return pd.read_sql(statement, session.bind)

    return statement


def get_trips(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    trip_cols: list[str] = [],
    get_df: bool = True,
    custom_filtering: dict = None,
) -> Union[pd.DataFrame, sqlalchemy.sql.selectable.Select]:
    """
    Query fct_scheduled_trips

    Must supply a list of feed_keys returned from
    schedule_daily_feed_to_gtfs_dataset_name() or subset of those results.
    """
    check_operator_feeds(operator_feeds)

    search_conditions = [
        FctScheduledTrips.service_date == selected_date,
        or_(FctScheduledTrips.feed_key.in_(operator_feeds), FctScheduledTrips.name.in_(operator_feeds)),
    ]

    for k, v in (custom_filtering or {}).items():
        search_conditions.append(getattr(FctScheduledTrips, k).in_(v))

    statement = select(FctScheduledTrips).where(and_(*search_conditions))

    # subset of columns must happen after Metrolink fix...
    # otherwise, the Metrolink fix may depend on more columns that
    # get subsetted out
    columns = []

    for column in trip_cols:
        columns.append(getattr(FctScheduledTrips, column))

    if get_df:
        with DBSession() as session:
            metrolink_feed_key_name_df = get_metrolink_feed_key(selected_date=selected_date, get_df=True)
            metrolink_empty = metrolink_feed_key_name_df.empty
            if not metrolink_empty:
                metrolink_feed_key = metrolink_feed_key_name_df.feed_key.iloc[0]
                metrolink_name = metrolink_feed_key_name_df.name.iloc[0]
            else:
                print(f"could not get metrolink feed on {selected_date}!")
            # Handle Metrolink when we need to
            if not metrolink_empty and ((metrolink_feed_key in operator_feeds) or (metrolink_name in operator_feeds)):
                metrolink_trips_statement = statement.where(FctScheduledTrips.feed_key == metrolink_feed_key)
                not_metrolink_trips_statement = statement.where(FctScheduledTrips.feed_key != metrolink_feed_key)
                metrolink_trips = pd.read_sql(metrolink_trips_statement, session.bind)
                not_metrolink_trips = pd.read_sql(not_metrolink_trips_statement, session.bind)

                # Fix Metrolink trips as a pd.DataFrame, then concatenate
                # This means that LazyTbl output will not show correct results
                corrected_metrolink = fill_in_metrolink_trips_df_with_shape_id(metrolink_trips, metrolink_feed_key)

                return pd.concat([not_metrolink_trips, corrected_metrolink], axis=0, ignore_index=True)[
                    trip_cols
                ].reset_index(drop=True)

            elif metrolink_empty or (metrolink_feed_key not in operator_feeds):
                statement = statement.with_only_columns(*columns) if len(columns) else statement
                return pd.read_sql(statement, session.bind)

    return statement.with_only_columns(*columns) if len(columns) else statement


def get_shapes(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    shape_cols: list[str] = [],
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
    custom_filtering: dict = None,
) -> Union[gpd.GeoDataFrame | sqlalchemy.sql.selectable.Select]:
    """
    Query fct_daily_scheduled_shapes.

    Must supply a list of feed_keys returned from
    schedule_daily_feed_to_gtfs_dataset_name() or subset of those results.
    """
    check_operator_feeds(operator_feeds)

    search_conditions = [
        FctDailyScheduledShapes.service_date == selected_date,
        FctDailyScheduledShapes.feed_key.in_(operator_feeds),
    ]

    for k, v in (custom_filtering or {}).items():
        search_conditions.append(getattr(FctDailyScheduledShapes, k).in_(v))

    statement = select(FctDailyScheduledShapes).where(and_(*search_conditions))

    if get_df:
        with DBSession() as session:
            shapes = pd.read_sql(statement, session.bind)

        # maintain usual behaviour of returning all in absence of subset param
        # must first drop pt_array since it's replaced by make_routes_gdf
        shapes_gdf = geography_utils.make_routes_gdf(shapes, crs=crs)[shape_cols + ["geometry"]]

        return shapes_gdf

    else:
        columns = {func.ST_ASBINARY(FctDailyScheduledShapes.pt_array)}

        for column in shape_cols:
            columns.add(getattr(FctDailyScheduledShapes, column))

        return statement.with_only_columns(*list(columns))


def get_stops(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    stop_cols: list[str] = [],
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
    custom_filtering: dict = None,
) -> Union[gpd.GeoDataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Query fct_daily_scheduled_stops.

    Must supply a list of feed_keys or organization names returned from
    schedule_daily_feed_to_gtfs_dataset_name() or subset of those results.
    """
    check_operator_feeds(operator_feeds)

    search_conditions = [
        FctDailyScheduledStops.service_date == selected_date,
        FctDailyScheduledStops.feed_key.in_(operator_feeds),
    ]

    for k, v in (custom_filtering or {}).items():
        search_conditions.append(getattr(FctDailyScheduledStops, k).in_(v))

    statement = select(FctDailyScheduledStops).where(and_(*search_conditions))

    if stop_cols and len(stop_cols):
        columns = [FctDailyScheduledStops.pt_geom]

        for column in stop_cols:
            columns.append(getattr(FctDailyScheduledStops, column))

        statement = statement.with_only_columns(*columns)

    if get_df:
        with DBSession() as session:
            stops = pd.read_sql(statement, session.bind)

        geom = [shapely.wkt.loads(x) for x in stops.pt_geom]
        stops = gpd.GeoDataFrame(stops, geometry=geom, crs="EPSG:4326").to_crs(crs).drop(columns="pt_geom")

        return stops
    else:
        return statement


def hour_tuple_to_seconds(hour_tuple: tuple[int]) -> tuple[int]:
    """
    If given a tuple(start_hour, end_hour), it will return
    tuple of (start_seconds, end_seconds)
    Let's use this to filter with the arrival_time and departure_time .

    For just 1 hour, use the same hour for start and end in the tuple.
    Ex: departure_hours = (12, 12)
    """
    SEC_IN_HOUR = 60 * 60
    start_sec = hour_tuple[0] * SEC_IN_HOUR
    end_sec = hour_tuple[1] * SEC_IN_HOUR - 1  # 1 sec before the next hour

    return (start_sec, end_sec)


def get_stop_times(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    stop_time_cols: list[str] = [],
    get_df: bool = True,
    trip_df: pd.DataFrame = None,
    custom_filtering: dict = None,
) -> Union[pd.DataFrame, sqlalchemy.sql.selectable.Select]:
    """
    Download stop times table for operator on a day.

    Allow a pre-existing trips table to be supplied.
    If not, run a fresh trips query.

    get_df: bool.
            If True, return pd.DataFrame
    """
    check_operator_feeds(operator_feeds)
    trip_id_cols = ["feed_key", "trip_id"]

    if trip_df is None:
        # Grab the trips for that day
        trip_df = get_trips(selected_date=selected_date, operator_feeds=operator_feeds, trip_cols=trip_id_cols)

    trip_df = trip_df[trip_id_cols]

    columns = [DimStopTime]

    if stop_time_cols and len(stop_time_cols):
        columns = []
        columns.append(DimStopTime.arrival_sec) if "arrival_sec" not in stop_time_cols else None
        columns.append(DimStopTime.departure_sec) if "departure_sec" not in stop_time_cols else None

        for column in stop_time_cols:
            columns.append(getattr(DimStopTime, column))

    search_conditions = [DimStopTime.feed_key.in_(operator_feeds), DimStopTime.trip_id.in_(trip_df.trip_id)]

    for k, v in (custom_filtering or {}).items():
        search_conditions.append(getattr(DimStopTime, k).in_(v))

    statement = select(*columns).where(and_(*search_conditions))

    if get_df:
        with DBSession() as session:
            stop_times = pd.read_sql(statement, session.bind)

        # Since we can parse by arrival or departure hour, let's
        # make it available when df is returned
        stop_times = stop_times.assign(
            arrival_hour=pd.to_datetime(stop_times.arrival_sec, unit="s").dt.hour,
            departure_hour=pd.to_datetime(stop_times.departure_sec, unit="s").dt.hour,
        )

        if stop_time_cols and len(stop_time_cols):
            stop_times = stop_times[stop_time_cols + ["arrival_hour", "departure_hour"]]

        return stop_times

    return statement


def filter_to_public_schedule_gtfs_dataset_keys(get_df: bool = False) -> list:
    """
    Return a list of schedule_gtfs_dataset_keys that have
    private_dataset == None.
    private_dataset holds values:True or None, no False.
    """
    dim_gtfs_datasets = schedule_rt_utils.filter_dim_gtfs_datasets(
        keep_cols=["key", "name", "private_dataset"],
        custom_filtering={
            "type": ["schedule"],
        },
        get_df=True,
    ) >> filter(_.private_dataset != True)

    if get_df:
        return dim_gtfs_datasets
    else:
        return dim_gtfs_datasets.gtfs_dataset_key.unique().tolist()
