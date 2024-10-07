"""
Concatenate the high-level operator stats.
Produce a single row for each operator-date we have.
This comprises first section of GTFS Digest.
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from segment_speed_utils import time_series_utils
from shared_utils import gtfs_utils_v2, publish_utils
from merge_data import merge_in_standardized_route_names
from update_vars import GTFS_DATA_DICT, SCHED_GCS, RT_SCHED_GCS

sort_cols = ["schedule_gtfs_dataset_key", "service_date"]

def concatenate_operator_stats(
    date_list: list
) -> pd.DataFrame:
    FILE = GTFS_DATA_DICT.schedule_tables.operator_scheduled_stats
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df


def concatenate_operator_routes( 
    date_list: list
) -> gpd.GeoDataFrame:
    FILE = GTFS_DATA_DICT.schedule_tables.operator_routes

    df = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        FILE,
        date_list,
        data_type = "gdf",
    ).sort_values(sort_cols).reset_index(drop=True)   
    
    return df


def operator_category_counts_by_date() -> pd.DataFrame:
    """
    Take the merged digest df with schedule vs RT metrics
    and count how many trips are 
    schedule_only, vp_only, and schedule_and_vp.
    
    Make sure that we only select 1 time period 
    and for schedule_and_vp, we select n_vp_trips (because not all
    scheduled trips may have vp).
    """
    INPUT = GTFS_DATA_DICT.digest_tables.route_schedule_vp
    
    operator_category_cols = [
        "schedule_gtfs_dataset_key", "service_date",
        "sched_rt_category"
    ]

    df = pd.read_parquet(
        f"{RT_SCHED_GCS}{INPUT}.parquet",
        filters = [[("time_period", "==", "all_day")]],
        columns = operator_category_cols + ["route_id", "direction_id", 
             "n_scheduled_trips", "n_vp_trips"]
    )
    
    # Sum by operator (across route-dir rows, and count how many trips)
    df2 = (df.groupby(operator_category_cols)
           .agg({"n_scheduled_trips": "sum",
                "n_vp_trips": "sum"})
           .reset_index()
    )
    
    # schedule only and vp only are straightforward, take
    # their corresponding n_scheduled_trips or n_vp_trips column
    sched_or_vp_only = df2.loc[
        df2.sched_rt_category!="schedule_and_vp"
    ]

    sched_or_vp_only = sched_or_vp_only.assign(
        n_trips = sched_or_vp_only[
            ["n_scheduled_trips", "n_vp_trips"]].sum(axis=1)
    )

    # for both, we'll only count the trips with vp (<= n_scheduled_trips)
    sched_rt = df2.loc[
        df2.sched_rt_category== "schedule_and_vp"
    ].rename(
        columns = {"n_vp_trips": "n_trips"})

    operator_category_counts = pd.concat(
        [sched_or_vp_only, sched_rt], 
        axis=0, ignore_index=True
    ).sort_values(
        operator_category_cols
    )[operator_category_cols + ["n_trips"]].reset_index(drop=True)
    
    return operator_category_counts

def concatenate_operator_level_metrics(
    date_list: list
) -> pd.DataFrame:
    """
    Get spatial accuracy and VP per Minute metrics on the
    operator-service_date grain.
    """
    FILE = f"{GTFS_DATA_DICT.rt_vs_schedule_tables.vp_operator_metrics}"
    
    df = time_series_utils.concatenate_datasets_across_dates(
        RT_SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df


if __name__ == "__main__":

    from shared_utils import rt_dates
    
    analysis_date_list = rt_dates.y2024_dates + rt_dates.y2023_dates 
    
    OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map
    OPERATOR_METRICS = GTFS_DATA_DICT.digest_tables.operator_metrics
    SCHED_RT_CATEGORY = GTFS_DATA_DICT.digest_tables.operator_sched_rt
    CROSSWALK = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk
    
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    # Concat operator metrics.
    operator_metrics = concatenate_operator_level_metrics(analysis_date_list)
    operator_metrics.to_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_METRICS}.parquet"
    )
    
    # Concat operator profiles
    df = concatenate_operator_stats(analysis_date_list)
    
    ntd_cols = [
        "schedule_gtfs_dataset_key",
        "caltrans_district",
        "counties_served",
        "service_area_sq_miles",
        "hq_city",
        "uza_name",
        "service_area_pop",
        "organization_type",
        "primary_uza",
        "reporter_type"
    ]
    
    # Merge in NTD data. 
    crosswalk_df = (
        time_series_utils.concatenate_datasets_across_dates(
            SCHED_GCS,
            CROSSWALK,
            analysis_date_list,
            data_type="df",
            columns=ntd_cols
        )
        .sort_values(["service_date"])
        .reset_index(drop=True)
    )
    
    # Merge
    merge_cols = ["schedule_gtfs_dataset_key", "service_date"]
    op_profiles_df1 = pd.merge(
        df, 
        crosswalk_df, 
        on = merge_cols, 
        how = "left"
    )
    
    # Drop duplicates created after merging
    op_profiles_df2 = (
        op_profiles_df1
        .pipe(
            publish_utils.exclude_private_datasets, 
            col = "schedule_gtfs_dataset_key", 
            public_gtfs_dataset_keys = public_feeds
        ).drop_duplicates(subset = list(op_profiles_df1.columns))
    .reset_index(drop = True))

    op_profiles_df2.to_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}.parquet"
    )
    
    gdf = concatenate_operator_routes(
        analysis_date_list
    ).pipe(
        merge_in_standardized_route_names
    ).pipe(
        publish_utils.exclude_private_datasets, 
        col = "schedule_gtfs_dataset_key", 
        public_gtfs_dataset_keys = public_feeds
    )

    utils.geoparquet_gcs_export(
        gdf,
        RT_SCHED_GCS,
        OPERATOR_ROUTE
    )
    
    operator_category_counts = operator_category_counts_by_date().pipe(
        publish_utils.exclude_private_datasets, 
        col = "schedule_gtfs_dataset_key", 
        public_gtfs_dataset_keys = public_feeds    
    )
    
    operator_category_counts.to_parquet(
        f"{RT_SCHED_GCS}{SCHED_RT_CATEGORY}.parquet"
    )
    
    