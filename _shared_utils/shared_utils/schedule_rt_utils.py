"""
Functions to bridge GTFS schedule and RT.
"""
from typing import Literal, Union

import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd
import siuba  # for type hints
from calitp_data_analysis.tables import tbls
from shared_utils import gtfs_utils_v2
from siuba import *

PACIFIC_TIMEZONE = "US/Pacific"


def localize_timestamp_col(df: dd.DataFrame, timestamp_col: Union[str, list]) -> dd.DataFrame:
    """
    RT vehicle timestamps are given in UTC.
    Localize these to Pacific Time.
    """
    # https://stackoverflow.com/questions/62992863/trying-to-convert-aware-local-datetime-to-naive-local-datetime-in-panda-datafram

    if isinstance(timestamp_col, str):
        timestamp_col = [timestamp_col]

    for c in timestamp_col:
        if isinstance(df, (dd.DataFrame, dg.GeoDataFrame)):
            localized_timestamp_col = (
                dd.to_datetime(df[c], utc=True)
                .dt.tz_convert(PACIFIC_TIMEZONE)
                .apply(lambda t: t.replace(tzinfo=None), meta=(None, "datetime64[ns]"))
            )
        elif isinstance(df, (pd.DataFrame, gpd.GeoDataFrame)):
            localized_timestamp_col = (
                pd.to_datetime(df[c], utc=True).dt.tz_convert(PACIFIC_TIMEZONE).apply(lambda t: t.replace(tzinfo=None))
            )

        df[f"{c}_local"] = localized_timestamp_col

    return df


def get_rt_schedule_feeds_crosswalk(
    date: str, keep_cols: list, get_df: bool = True, custom_filtering: dict = None
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Get fct_daily_rt_feeds, which provides the schedule_feed_key
    to use when merging with schedule data.
    """
    fct_rt_feeds = tbls.mart_gtfs.fct_daily_rt_feed_files() >> filter(_.date == date)

    if get_df:
        fct_rt_feeds = (
            fct_rt_feeds
            >> collect()
            >> gtfs_utils_v2.filter_custom_col(custom_filtering)
            >> gtfs_utils_v2.subset_cols(keep_cols)
        )

    return fct_rt_feeds >> gtfs_utils_v2.subset_cols(keep_cols)


def get_schedule_gtfs_dataset_key(date: str, get_df: bool = True) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Use fct_daily_feed_scheduled_service to find
    the gtfs_dataset_key that corresponds to the feed_key.
    """
    schedule_feed_to_rt_key = (
        tbls.mart_gtfs.fct_daily_feed_scheduled_service_summary()
        >> filter(_.service_date == date)
        >> select(_.gtfs_dataset_key, _.feed_key)
    )

    if get_df:
        schedule_feed_to_rt_key = schedule_feed_to_rt_key >> collect()

    return schedule_feed_to_rt_key


def filter_dim_gtfs_datasets(
    keep_cols: list[str] = ["key", "name", "type", "regional_feed_type", "uri", "base64_url"],
    custom_filtering: dict = None,
    get_df: bool = True,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """ """
    if "key" not in keep_cols:
        raise KeyError("Include key in keep_cols list")

    dim_gtfs_datasets = (
        tbls.mart_transit_database.dim_gtfs_datasets()
        >> filter(_.data_quality_pipeline == True)  # if True, we can use
        >> gtfs_utils_v2.filter_custom_col(custom_filtering)
        >> gtfs_utils_v2.subset_cols(keep_cols)
    )

    # rename columns to match our convention
    if "key" in keep_cols:
        dim_gtfs_datasets = dim_gtfs_datasets >> rename(gtfs_dataset_key="key")
    if "name" in keep_cols:
        dim_gtfs_datasets = dim_gtfs_datasets >> rename(gtfs_dataset_name="name")

    if get_df:
        dim_gtfs_datasets = dim_gtfs_datasets >> collect()

    return dim_gtfs_datasets


def get_organization_id(
    df: pd.DataFrame,
    date: str,
    merge_cols: list = [],
) -> pd.DataFrame:
    """
    Instead of using the GTFS dataset name (of the quartet), usually
    LA Metro Schedule, LA Metro Trip Updates, etc, always
    publish with the organization name, LA Metro.

    Input a date to filter down what feeds were valid.
    Merge columns must be defined and hold prefixes indicating the quartet.
    Ex: schedule_gtfs_dataset_key, vehicle_positions_gtfs_dataset_name
    """
    quartet = ["schedule", "vehicle_positions", "service_alerts", "trip_updates"]

    datasets = ["gtfs_dataset_key", "gtfs_dataset_name", "source_record_id"]

    # https://stackoverflow.com/questions/2541401/pairwise-crossproduct-in-python
    quartet_cols = [f"{q}_{d}" for q in quartet for d in datasets]

    # All the merge cols must be found in quartet_cols
    # This is flexible enough so we can take just gtfs_dataset_key or name to merge too
    if not all(c in quartet_cols for c in merge_cols):
        raise KeyError(
            "Unable to detect which GTFS quartet "
            f"these columns {df.columns}. "
            "Rename to [quartet]_gtfs_dataset_key, "
            "[quartet]_gtfs_dataset_name. "
            "Valid quartet values: schedule, vehicle_positions, "
            "trip_updates, or service_alerts."
        )
    else:
        dim_provider_gtfs_data = tbls.mart_transit_database.dim_provider_gtfs_data() >> distinct() >> collect()

        dim_provider_gtfs_data = localize_timestamp_col(dim_provider_gtfs_data, ["_valid_from", "_valid_to"])

        dim_provider_gtfs_data2 = dim_provider_gtfs_data >> filter(
            _._valid_from_local <= pd.to_datetime(date), _._valid_to_local >= pd.to_datetime(date)
        )

        sorting = [True for c in merge_cols]
        keep_cols = ["organization_source_record_id"]

        # Eventually, we need to move to 1 organization name, so there's
        # no fanout when we merge it on
        # Until then, handle it by dropping duplicates and pick 1 name
        dim_provider_gtfs_data2 = (
            dim_provider_gtfs_data2.sort_values(
                merge_cols + ["_valid_to", "_valid_from"], ascending=sorting + [False, False]
            )
            .drop_duplicates(merge_cols)
            .reset_index(drop=True)[merge_cols + keep_cols]
        )

        df2 = pd.merge(df, dim_provider_gtfs_data2, on=merge_cols, how="inner")
        # return dim_provider_gtfs_data2
        return df2


def filter_dim_organizations(
    date: str,
    keep_cols: list[str] = ["source_record_id", "caltrans_district"],
    custom_filtering: dict = None,
    get_df: bool = True,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Filter dim_organizations down to current record for organization.
    Caltrans district is associated with organization_source_record_id.
    """
    dim_orgs = (
        tbls.mart_transit_database.dim_organizations()
        >> gtfs_utils_v2.filter_custom_col(custom_filtering)
        >> filter(_._is_current == True)
        >> gtfs_utils_v2.subset_cols(keep_cols)
        >> rename(organization_source_record_id="source_record_id")
    )

    if get_df:
        dim_orgs = dim_orgs >> collect()

    return dim_orgs


def sample_gtfs_dataset_key_to_organization_crosswalk(
    df: pd.DataFrame,
    date: str,
    quartet_data: Literal["schedule", "vehicle_positions", "service_alerts", "trip_updates"],
    dim_gtfs_dataset_cols: list[str] = [
        "key",
        "name",
        "type",
        "source_record_id",
        "regional_feed_type",
        "base64_url",
        "uri",
    ],
    dim_organization_cols: list[str] = ["source_record_id", "name", "caltrans_district"],
) -> pd.DataFrame:
    """
    Get crosswalk from gtfs_dataset_key to certain quartet data identifiers
    like base64_url, uri, and organization identifiers
    like organization_source_record_id and caltrans_district.
    """
    id_cols = ["gtfs_dataset_key"]

    # If schedule feed_key is present, include it our crosswalk output
    if "feed_key" in df.columns:
        id_cols = ["feed_key"] + id_cols

    feeds = df[id_cols].drop_duplicates().reset_index(drop=True)

    # (1) Filter dim_gtfs_datasets by quartet and merge with the
    # gtfs_dataset_keys in df.
    dim_gtfs_datasets = filter_dim_gtfs_datasets(
        keep_cols=dim_gtfs_dataset_cols, custom_filtering={"type": [quartet_data]}, get_df=True
    )

    feeds_with_quartet_info = pd.merge(
        feeds,
        dim_gtfs_datasets,
        on="gtfs_dataset_key",
        how="inner",
    )

    # (2) Rename key/name to reflect the quartet
    GTFS_DATASET_RENAME_DICT = {
        "gtfs_dataset_key": f"{quartet_data}_gtfs_dataset_key",
        "gtfs_dataset_name": f"{quartet_data}_gtfs_dataset_name",
        "source_record_id": f"{quartet_data}_source_record_id",
    }

    feeds_with_quartet_info = feeds_with_quartet_info.rename(columns=GTFS_DATASET_RENAME_DICT)

    # (3) From quartet, get to organization name
    merge_cols = [i for i in feeds_with_quartet_info.columns if quartet_data in i]

    feeds_with_org_id = get_organization_id(feeds_with_quartet_info, date, merge_cols=merge_cols)

    # (4) Merge in dim_orgs to get caltrans_district
    ORG_RENAME_DICT = {"source_record_id": "organization_source_record_id", "name": "organization_name"}
    orgs = filter_dim_organizations(date, keep_cols=dim_organization_cols, get_df=True).rename(columns=ORG_RENAME_DICT)

    feeds_with_district = pd.merge(feeds_with_org_id, orgs, on="organization_source_record_id")

    return feeds_with_district


def sample_schedule_feed_key_to_organization_crosswalk(
    df: pd.DataFrame,
    date: str,
    quartet_data: Literal["schedule", "vehicle_positions", "service_alerts", "trip_updates"] = "schedule",
    **kwargs,
) -> pd.DataFrame:
    """
    From schedule data, using feed_key as primary key,
    grab the gtfs_dataset_key associated.
    Pass this through function to attach quartet data identifier columns
    and organization info.
    """
    # Start with schedule feed_key, and grab gtfs_dataset_key associated
    # with that feed_key
    feeds = df[["feed_key"]].drop_duplicates().reset_index(drop=True)

    crosswalk_feed_to_gtfs_dataset_key = get_schedule_gtfs_dataset_key(date, get_df=True)

    feeds_with_gtfs_dataset_key = pd.merge(
        feeds,
        crosswalk_feed_to_gtfs_dataset_key,
        on="feed_key",
        how="inner",
    )

    feeds_with_district = sample_gtfs_dataset_key_to_organization_crosswalk(
        feeds_with_gtfs_dataset_key, date, quartet_data=quartet_data, **kwargs
    )

    return feeds_with_district
