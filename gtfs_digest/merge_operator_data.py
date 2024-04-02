"""
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from segment_speed_utils import time_series_utils
from segment_speed_utils.project_vars import SCHED_GCS, RT_SCHED_GCS
from merge_data import merge_in_standardized_route_names

sort_cols = ["schedule_gtfs_dataset_key", "service_date"]

def concatenate_operator_stats(
    date_list: list
) -> pd.DataFrame:
    df = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        "operator_profiles/operator_scheduled_stats",
        date_list,
        data_type = "df",
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df


def concatenate_operator_routes( 
    date_list: list
) -> gpd.GeoDataFrame:
    df = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        "operator_profiles/operator_routes",
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
    operator_category_cols = [
        "schedule_gtfs_dataset_key", "service_date",
        "sched_rt_category"
    ]

    df = pd.read_parquet(
        f"{RT_SCHED_GCS}digest/schedule_vp_metrics.parquet",
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

if __name__ == "__main__":

    from shared_utils.rt_dates import y2023_dates, y2024_dates
    
    analysis_date_list = y2024_dates + y2023_dates 
    '''
    df = concatenate_operator_stats(analysis_date_list)
    
    df.to_parquet(
        f"{RT_SCHED_GCS}digest/operator_profiles.parquet"
    )
    
    gdf = concatenate_operator_routes(
        analysis_date_list
    ).pipe(merge_in_standardized_route_names)

    utils.geoparquet_gcs_export(
        gdf,
        RT_SCHED_GCS,
        "digest/operator_routes"
    )
    '''
    operator_category_counts = operator_category_counts_by_date()
    operator_category_counts.to_parquet(
        f"{RT_SCHED_GCS}"
        "digest/operator_schedule_rt_category.parquet"
    )