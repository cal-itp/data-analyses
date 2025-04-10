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

"""
Concatenating Functions 
"""
def concatenate_rt_vs_schedule_operator_metrics(
    date_list: list
) -> pd.DataFrame:
    
    FILE = f"{GTFS_DATA_DICT.rt_vs_schedule_tables.vp_operator_metrics}"
    
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

def concatenate_crosswalks(
    date_list: list
) -> pd.DataFrame:
    """
    Get crosswalk and selected NTD columns for certain dates.
    """
    FILE = f"{GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk}"
    
    ntd_cols = [
        "schedule_gtfs_dataset_key",
        "caltrans_district",
        "counties_served",
        "service_area_sq_miles",
        "hq_city",
        "service_area_pop",
        "organization_type",
        "primary_uza_name",
        "reporter_type"
    ]
        
    df = (
        time_series_utils.concatenate_datasets_across_dates(
            SCHED_GCS,
            FILE,
            date_list,
            data_type="df",
            columns=ntd_cols
        )
        .sort_values(["service_date"])
        .reset_index(drop=True)
    )
    
    return df

def concatenate_schedule_operator_metrics(
    date_list: list
) -> pd.DataFrame:
    """
    Get spatial accuracy and vehicle positions per minute metrics on the
    operator-service_date grain for certain dates.
    """
    FILE = GTFS_DATA_DICT.schedule_tables.operator_scheduled_stats
    
    df = time_series_utils.concatenate_datasets_across_dates(
        RT_SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df


if __name__ == "__main__":

    from shared_utils import rt_dates
    
    analysis_date_list = (
        rt_dates.y2024_dates + rt_dates.y2023_dates +
        rt_dates.y2025_dates
    )
    
    OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map
    
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    # Concat operator metrics.
    op_sched_metrics = concatenate_schedule_operator_metrics(analysis_date_list)
    
    # Concat operator profiles
    op_rt_sched_metrics = concatenate_rt_vs_schedule_operator_metrics(analysis_date_list)
    
    merge_cols = ["schedule_gtfs_dataset_key",
             "service_date"]
    
    # Merge the two together
    operator_profiles_df1 = pd.merge(op_sched_metrics, 
                                  op_rt_sched_metrics,
                                  on = merge_cols, 
                                  how = "outer")
 
    
    # Concat NTD/crosswalk
    crosswalk_df = concatenate_crosswalks(analysis_date_list)
    
    # Merge in NTD data. 
    op_profiles_df2 = pd.merge(
        operator_profiles_df1, 
        crosswalk_df, 
        on = merge_cols, 
        how = "left"
    )
    
    # Drop duplicates created after merging
    # Add more strigent drop duplicate criteria
    duplicate_cols = ["schedule_gtfs_dataset_key",
                     "vp_per_min_agency",
                     "spatial_accuracy_agency",
                     "service_date",
                     "organization_name",
                     "caltrans_district"]

    op_profiles_df3 = (
        op_profiles_df2
        .pipe(
            publish_utils.exclude_private_datasets, 
            col = "schedule_gtfs_dataset_key", 
            public_gtfs_dataset_keys = public_feeds
        ).drop_duplicates(subset = duplicate_cols)
    .reset_index(drop = True))

    op_profiles_df3.to_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}.parquet"
    )
    
    # Load in scheduled routes.
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