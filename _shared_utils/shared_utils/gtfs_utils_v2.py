"""
GTFS utils for v2 warehouse
"""

# import os
# os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

import datetime
from typing import Literal, Union

import geopandas as gpd
import pandas as pd
import shapely
import siuba  # need this to do type hint in functions
from calitp_data_analysis.tables import tbls
from shared_utils import geography_utils, schedule_rt_utils
from siuba import *

GCS_PROJECT = "cal-itp-data-infra"

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


def filter_feed_options(
    feed_option: Literal[
        "customer_facing",
        "use_subfeeds",
        "current_feeds",
        "include_precursor",
    ]
) -> siuba.dply.verbs.Pipeable:
    exclude_precursor = filter(_.regional_feed_type != "Regional Precursor Feed")

    if feed_option == "customer_facing":
        return filter(_.regional_feed_type != "Regional Subfeed") >> exclude_precursor

    elif feed_option == "use_subfeeds":
        return (
            filter(
                _["name"] != "Bay Area 511 Regional Schedule"
            )  # keep VCTC combined because the combined feed is the only feed
            >> exclude_precursor
        )

    elif feed_option == "current_feeds":
        return exclude_precursor

    elif feed_option == "include_precursor":
        return filter()
    else:
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


def get_metrolink_feed_key(selected_date: Union[str, datetime.date], get_df: bool = False) -> pd.DataFrame:
    """
    Get Metrolink's feed_key value.
    """
    metrolink_in_airtable = schedule_rt_utils.filter_dim_gtfs_datasets(
        keep_cols=["key", "name"], custom_filtering={"name": ["Metrolink Schedule"]}, get_df=False
    )

    metrolink_feed = (
        tbls.mart_gtfs.fct_daily_schedule_feeds()
        >> filter(_.date == selected_date)
        >> inner_join(_, metrolink_in_airtable, on=["gtfs_dataset_key", "gtfs_dataset_name"])
        >> rename(name=_.gtfs_dataset_name)
        >> subset_cols(["feed_key", "name"])
        >> collect()
    )

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
        "include_precursor_and_future",
    ] = "use_subfeeds",
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
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

    * include_precursor_and_future: include precursor feeds and future feeds.
                Caution: would result in duplicate organization names
    """
    # Get GTFS schedule datasets from Airtable
    dim_gtfs_datasets = schedule_rt_utils.filter_dim_gtfs_datasets(
        keep_cols=["key", "name", "type", "regional_feed_type"], custom_filtering={"type": ["schedule"]}, get_df=False
    ) >> rename(name="gtfs_dataset_name")

    # Merge on gtfs_dataset_key to get organization name
    fact_feeds = (
        tbls.mart_gtfs.fct_daily_schedule_feeds()
        >> filter(_.date == selected_date)
        >> inner_join(_, dim_gtfs_datasets, on="gtfs_dataset_key")
    )

    if get_df:
        fact_feeds = (
            fact_feeds
            >> collect()
            # apparently order matters - if this is placed before
            # the collect(), it filters out wayyyy too many
            >> filter_feed_options(feed_option)
        )

    return fact_feeds >> subset_cols(keep_cols)


def get_trips(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    trip_cols: list[str] = [],
    get_df: bool = True,
    custom_filtering: dict = None,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Query fct_scheduled_trips

    Must supply a list of feed_keys returned from
    schedule_daily_feed_to_gtfs_dataset_name() or subset of those results.
    """
    check_operator_feeds(operator_feeds)

    trips = (
        tbls.mart_gtfs.fct_scheduled_trips()
        >> filter_date(selected_date, date_col="service_date")
        >> filter_operator(operator_feeds, include_name=True)
        >> filter_custom_col(custom_filtering)
    )

    # subset of columns must happen after Metrolink fix...
    # otherwise, the Metrolink fix may depend on more columns that
    # get subsetted out
    if get_df:
        metrolink_feed_key_name_df = get_metrolink_feed_key(selected_date=selected_date, get_df=True)
        metrolink_empty = metrolink_feed_key_name_df.empty
        if not metrolink_empty:
            metrolink_feed_key = metrolink_feed_key_name_df.feed_key.iloc[0]
            metrolink_name = metrolink_feed_key_name_df.name.iloc[0]
        else:
            print(f"could not get metrolink feed on {selected_date}!")
        # Handle Metrolink when we need to
        if not metrolink_empty and ((metrolink_feed_key in operator_feeds) or (metrolink_name in operator_feeds)):
            metrolink_trips = trips >> filter(_.feed_key == metrolink_feed_key) >> collect()
            not_metrolink_trips = trips >> filter(_.feed_key != metrolink_feed_key) >> collect()

            # Fix Metrolink trips as a pd.DataFrame, then concatenate
            # This means that LazyTbl output will not show correct results
            corrected_metrolink = fill_in_metrolink_trips_df_with_shape_id(metrolink_trips, metrolink_feed_key)

            trips = pd.concat([not_metrolink_trips, corrected_metrolink], axis=0, ignore_index=True)[
                trip_cols
            ].reset_index(drop=True)

        elif metrolink_empty or (metrolink_feed_key not in operator_feeds):
            trips = trips >> subset_cols(trip_cols) >> collect()

    return trips >> subset_cols(trip_cols)


def get_shapes(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    shape_cols: list[str] = [],
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
    custom_filtering: dict = None,
) -> gpd.GeoDataFrame:
    """
    Query fct_daily_scheduled_shapes.

    Must supply a list of feed_keys returned from
    schedule_daily_feed_to_gtfs_dataset_name() or subset of those results.
    """
    check_operator_feeds(operator_feeds)

    # If pt_array is not kept in the final, we still need it
    # to turn this into a gdf
    if "pt_array" not in shape_cols:
        shape_cols_with_geom = shape_cols + ["pt_array"]
    elif shape_cols:
        shape_cols_with_geom = shape_cols[:]

    shapes = (
        tbls.mart_gtfs.fct_daily_scheduled_shapes()
        >> filter_date(selected_date, date_col="service_date")
        >> filter_operator(operator_feeds, include_name=False)
        >> filter_custom_col(custom_filtering)
    )

    if get_df:
        shapes = shapes >> collect()

        # maintain usual behaviour of returning all in absence of subset param
        # must first drop pt_array since it's replaced by make_routes_gdf
        shapes_gdf = geography_utils.make_routes_gdf(shapes, crs=crs)[shape_cols + ["geometry"]]

        return shapes_gdf

    else:
        return shapes >> subset_cols(shape_cols_with_geom)


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

    # If pt_geom is not kept in the final, we still need it
    # to turn this into a gdf
    if (stop_cols) and ("pt_geom" not in stop_cols):
        stop_cols_with_geom = stop_cols + ["pt_geom"]
    else:
        stop_cols_with_geom = stop_cols[:]

    stops = (
        tbls.mart_gtfs.fct_daily_scheduled_stops()
        >> filter_date(selected_date, date_col="service_date")
        >> filter_operator(operator_feeds, include_name=False)
        >> filter_custom_col(custom_filtering)
        >> subset_cols(stop_cols_with_geom)
    )

    if get_df:
        stops = stops >> collect()

        geom = [shapely.wkt.loads(x) for x in stops.pt_geom]

        stops = gpd.GeoDataFrame(stops, geometry=geom, crs="EPSG:4326").to_crs(crs).drop(columns="pt_geom")

        return stops
    else:
        return stops >> subset_cols(stop_cols)


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


def filter_start_end_ts(time_filters: dict, time_col: Literal["arrival", "departure"]) -> siuba.dply.verbs.Pipeable:
    """
    For arrival or departure, grab the hours to subset and
    convert the (start_hour, end_hour) tuple into seconds,
    and return the siuba filter
    """
    desired_hour_tuple = time_filters[time_col]
    (start_sec, end_sec) = hour_tuple_to_seconds(desired_hour_tuple)

    return filter(_[f"{time_col}_sec"] >= start_sec, _[f"{time_col}_sec"] <= end_sec)


def get_stop_times(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    stop_time_cols: list[str] = [],
    get_df: bool = False,
    trip_df: Union[pd.DataFrame, siuba.sql.verbs.LazyTbl] = None,
    custom_filtering: dict = None,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
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
        trips_on_day = get_trips(selected_date=selected_date, trip_cols=trip_id_cols, get_df=False)

    elif trip_df is not None:
        if isinstance(trip_df, siuba.sql.verbs.LazyTbl):
            trips_on_day = trip_df >> select(trip_id_cols)

        # Have to handle pd.DataFrame separately later
        elif isinstance(trip_df, pd.DataFrame):
            trips_on_day = trip_df[trip_id_cols]

    if not isinstance(trips_on_day, pd.DataFrame):
        stop_times = (
            tbls.mart_gtfs.dim_stop_times()
            >> filter_operator(operator_feeds, include_name=True)
            >> inner_join(_, trips_on_day, on=trip_id_cols)
            >> filter_custom_col(custom_filtering)
            >> subset_cols(stop_time_cols)
        )

    elif isinstance(trips_on_day, pd.DataFrame):
        # if trips is pd.DataFrame, can't use inner_join because that needs LazyTbl
        # on both sides. Use .isin then
        stop_times = (
            tbls.mart_gtfs.dim_stop_times()
            >> filter_operator(operator_feeds, include_name=False)
            >> filter(_.trip_id.isin(trips_on_day.trip_id))
            >> filter_custom_col(custom_filtering)
            >> subset_cols(stop_time_cols)
        )

    if get_df:
        stop_times = stop_times >> collect()

        # Since we can parse by arrival or departure hour, let's
        # make it available when df is returned
        stop_times = stop_times.assign(
            arrival_hour=pd.to_datetime(stop_times.arrival_sec, unit="s").dt.hour,
            departure_hour=pd.to_datetime(stop_times.departure_sec, unit="s").dt.hour,
        )

    return stop_times
