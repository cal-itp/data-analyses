"""
Migrate gtfs_utils from v1 to v2 warehouse.

Changes from v1 to v2:
1. new feed_key to organization_name table created.
STATUS: stays in gtfs_utils_v2 for now

    Should probably stage in dbt anyway to see what feeds / names
    are there for the same day?
    Rather than subsetting by feed_keys, join this new table to
    all subsequent queries, so analysts can query by org name too.

2. get_trips: expand and move to dbt
Status: dbt table in progress, function updated here.

    Rework trips query to better reflect how we use this query as our
    base query.
    Bring in route columns we might use to subset
    (route_type, route_short_name, route_long_name).

    Move this joining into dbt to be a mart_trips table that analysts go to first.

3. get_route_info: deprecate.
Statsu: dbt table in progress, removed from here.

    Instead, expand the trips query to incorporate our most-used
    route columns from dim_routes to help with subsetting. See #2

4. get_route_shapes: change make_routes_gdf to take pd.DataFrame,
    keep in gtfs_utils_v2
Status: dbt table in progress, function updated here

    Use dask.apply to apply the make_linestring function.
    Move this new make_routes_gdf over to geography_utils when it's ready
    and replace that.

5. get_stops: move to dbt
Status: dbt table in progress, function updated here

    set this up so that point geometry can be created in dbt.
    Add a mart_stops table that analysts go to first.

6. get_stop_times: TODO
    TODO. think more on what functions typically come from stop_times.
    Aggregation, for sure, and that can be handled if it returns dask.dataframe
    as in previous get_stop_times.

    With new timestamp that's numeric, we might want to add subsetting by departure
    hour on top of that so we don't have to do the calculation of seconds to
    departure hour more.

"""

# import os
# os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

import datetime
from typing import Union

import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
import siuba  # need this to do type hint in functions
from calitp.tables import tbls
from shared_utils import geography_utils
from siuba import *

GCS_PROJECT = "cal-itp-data-infra"

# YESTERDAY_DATE = datetime.date.today() + datetime.timedelta(days=-1)

# ----------------------------------------------------------------#
# Convenience siuba filtering functions for querying
# ----------------------------------------------------------------#


def filter_operator(operator_feeds: list) -> siuba.dply.verbs.Pipeable:
    """
    Filter if operator_list is present.
    Otherwise, skip.
    """
    return filter(_.feed_key.isin(operator_feeds))


def filter_date(selected_date: Union[str, datetime.date]) -> siuba.dply.verbs.Pipeable:
    return filter(_.service_date == selected_date)


def subset_cols(cols: list) -> siuba.dply.verbs.Pipeable:
    """
    Select subset of columns, if column list is present.
    Otherwise, skip.
    """
    if cols is not None:
        return select(*cols)
    else:
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
        raise ValueError("Supply list of feed keys!")


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
    selected_date: Union[str, datetime.date], get_df: bool = False
) -> pd.DataFrame:
    """
    Get Metrolink's feed_key value.
    """
    metrolink_in_airtable = get_transit_organizations_gtfs_dataset_keys(
        keep_cols=["key", "name"], custom_filtering={"name": ["Metrolink Schedule"]}
    )

    metrolink_feed = (
        tbls.mart_gtfs.fct_daily_schedule_feeds()
        >> filter(_.date == selected_date, _.is_future == False)
        >> inner_join(_, metrolink_in_airtable, on="gtfs_dataset_key")
        >> subset_cols(["feed_key", "name"])
        >> collect()
    )

    if get_df:
        return metrolink_feed
    else:
        return metrolink_feed.feed_key.iloc[0]


def fill_in_metrolink_trips_df_with_shape_id(
    trips: pd.DataFrame, metrolink_feed_key: str
) -> pd.DataFrame:
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


def get_transit_organizations_gtfs_dataset_keys(
    keep_cols: list[str], custom_filtering: dict = None
):
    """
    From Airtable GTFS datasets, get the datasets (and gtfs_dataset_key)
    for usable feeds.

    With no filters, all available dataset quartets are returned.
    """
    dim_gtfs_datasets = (
        tbls.mart_transit_database.dim_gtfs_datasets()
        >> filter(_.data_quality_pipeline == True)  # if True, we can use
        >> subset_cols(keep_cols)
        >> filter_custom_col(custom_filtering)
        >> rename(gtfs_dataset_key="key")
    )

    return dim_gtfs_datasets


def schedule_daily_feed_to_organization(
    selected_date: Union[str, datetime.date],
    keep_cols: list[str] = None,
    get_df: bool = True,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Select a date, find what feeds are present, and
    merge in organization name.

    Analysts start here to decide how to filter down.

    Custom filtering doesn't work well here...esp with
    None or booleans, which don't unpack well, even as items in a list.
    Returns incorrect results.

    Analyst would manually include or exclude feeds based on any of the columns.
    """
    # Get GTFS schedule datasets from Airtable
    dim_gtfs_datasets = get_transit_organizations_gtfs_dataset_keys(
        keep_cols=["key", "name", "type", "regional_feed_type"],
        custom_filtering={"type": ["schedule"]},
    )

    # Merge on gtfs_dataset_key to get organization name
    fact_feeds = (
        tbls.mart_gtfs.fct_daily_schedule_feeds()
        >> filter(_.date == selected_date, _.is_future == False)
        >> inner_join(_, dim_gtfs_datasets, on="gtfs_dataset_key")
        >> subset_cols(keep_cols)
    )

    if get_df:
        fact_feeds = fact_feeds >> collect()

    return fact_feeds


def get_trips(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    trip_cols: list[str] = None,
    get_df: bool = True,
    custom_filtering: dict = None,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Query fct_daily_scheduled_trips

    Must supply a list of feed_keys returned from
    schedule_daily_feed_to_organization() or subset of those results.
    """
    check_operator_feeds(operator_feeds)

    trips = (
        tbls.mart_gtfs.fct_daily_scheduled_trips()
        >> filter_date(selected_date)
        >> filter_operator(operator_feeds)
        >> filter_custom_col(custom_filtering)
    )

    # subset of columns must happen after Metrolink fix...otherwise,
    # Metrolink fix may depend on more columns that after, we're not interested in

    # Handle Metrolink when we need to
    if (metrolink_feed_key in operator_feeds) and (get_df):
        metrolink_feed_key = gtfs_utils_v2.get_metrolink_feed_key(
            selected_date=analysis_date, get_df=False
        )

        metrolink_trips = trips >> filter(_.feed_key == metrolink_feed_key) >> collect()
        not_metrolink_trips = (
            trips >> filter(_.feed_key != metrolink_feed_key) >> collect()
        )

        # Fix Metrolink trips as a pd.DataFrame, then concatenate
        # This means that LazyTbl output will not show correct results
        corrected_metrolink = fill_in_metrolink_trips_df_with_shape_id(metrolink_trips)

        trips = pd.concat(
            [not_metrolink_trips, corrected_metrolink], axis=0, ignore_index=True
        )[trip_cols].reset_index(drop=True)

    elif (metrolink_feed_key not in operator_feeds) and (get_df):
        trips = trips >> subset_cols(trip_cols) >> collect()

    return trips >> subset_cols(trip_cols)


def make_routes_gdf(
    df: pd.DataFrame,
    crs: str = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """
    Parameters:

    crs: str, a projected coordinate reference system.
        Defaults to EPSG:4326 (WGS84)
    """
    # Use apply to use map_partitions
    # https://stackoverflow.com/questions/70829491/dask-map-partitions-meta-when-using-lambda-function-to-add-column
    ddf = dd.from_pandas(df, npartitions=1)

    ddf["geometry"] = ddf.pt_array.apply(
        geography_utils.make_linestring, meta=("geometry", "geometry")
    )
    shapes = ddf.compute()

    # convert to geopandas; re-project if needed
    gdf = gpd.GeoDataFrame(
        shapes.drop(columns="pt_array"), geometry="geometry", crs=geography_utils.WGS84
    ).to_crs(crs)

    return gdf


def get_shapes(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    shape_cols: list[str] = None,
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
    custom_filtering: dict = None,
) -> gpd.GeoDataFrame:
    """
    Query fct_daily_scheduled_shapes.

    Must supply a list of feed_keys returned from
    schedule_daily_feed_to_organization() or subset of those results.
    """
    check_operator_feeds(operator_feeds)

    shapes = (
        tbls.mart_gtfs.fct_daily_scheduled_shapes()
        >> filter_date(selected_date)
        >> filter_operator(operator_feeds)
        >> filter_custom_col(custom_filtering)
    )

    if get_df:
        shapes2 = shapes.compute()
        shapes_gdf = make_routes_gdf(shapes2, crs=crs)[shape_cols]

        return shapes_gdf

    else:
        return shapes >> subset_cols(shape_cols)


def get_stops(
    selected_date: Union[str, datetime.date],
    operator_feeds: list[str] = [],
    stop_cols: list[str] = None,
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
    custom_filtering: dict = None,
) -> Union[gpd.GeoDataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Query fct_daily_scheduled_stops.

    Must supply a list of feed_keys returned from
    schedule_daily_feed_to_organization() or subset of those results.
    """
    check_operator_feeds(operator_feeds)

    stops = (
        tbls.mart_gtfs.fct_daily_scheduled_stops()
        >> filter_date(selected_date)
        >> filter_operator(operator_feeds)
        >> filter_custom_col(custom_filtering)
        >> subset_cols(stop_cols)
    )

    if get_df:
        stops = stops >> collect()
        stops = stops.to_crs(crs)

    return stops


# TODO: stop_times.
