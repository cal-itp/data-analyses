"""
Functions to bridge GTFS schedule and RT.
"""
import datetime as dt
from typing import Union

import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd
from calitp_data_analysis.tables import tbls
from shared_utils import gtfs_utils_v2  # rt_utils
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
    the schedule gtfs_dataset_key that corresponds to the feed_key.
    """
    schedule_feed_to_rt_key = (
        tbls.mart_gtfs.fct_daily_feed_scheduled_service_summary()
        >> filter(_.service_date == date)
        >> select(_.schedule_gtfs_dataset_key == _.gtfs_dataset_key, _.feed_key)
    )

    if get_df:
        schedule_feed_to_rt_key = schedule_feed_to_rt_key >> collect()

    return schedule_feed_to_rt_key


def filter_dim_gtfs_dataset(
    date: str,
    keep_cols: list[str] = [],
    custom_filtering: dict = None,
    get_df: bool = True,
):
    dim_gtfs_datasets = (
        tbls.mart_transit_database.dim_gtfs_datasets()
        >> filter(_.data_quality_pipeline == True)  # if True, we can use
        >> gtfs_utils_v2.subset_cols(keep_cols)
        >> gtfs_utils_v2.filter_custom_col(custom_filtering)
        >> rename(gtfs_dataset_key="key")
    )

    if get_df:
        dim_gtfs_datasets = dim_gtfs_datasets >> collect()

    return dim_gtfs_datasets


def get_organization_name(
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
        analysis_dt = dt.datetime.combine(date, dt.time(0))

        dim_provider_gtfs_data = (
            tbls.mart_transit_database.dim_provider_gtfs_data()
            >> filter(_._valid_from <= pd.to_datetime(analysis_dt), _._valid_to >= pd.to_datetime(analysis_dt))
            >> distinct()
            >> collect()
        )

        sorting = [True for c in merge_cols]
        keep_cols = ["organization_source_record_id", "organization_name", "regional_feed_type"]
        # Eventually, we need to move to 1 organization name, so there's
        # no fanout when we merge it on
        # Until then, handle it by dropping duplicates and pick 1 name
        dim_provider_gtfs_data2 = (
            dim_provider_gtfs_data.sort_values(
                merge_cols + ["_valid_to", "_valid_from"], ascending=sorting + [False, False]
            )
            .drop_duplicates(merge_cols)
            .reset_index(drop=True)[merge_cols + keep_cols]
        )

        df2 = pd.merge(df, dim_provider_gtfs_data2, on=merge_cols, how="inner")
        # return dim_provider_gtfs_data2
        return df2
