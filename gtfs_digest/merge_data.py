"""
Merge datasets across dates to create time-series data
for GTFS digest.
Grain is operator-service_date-route-direction-time_period.
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from segment_speed_utils import gtfs_schedule_wrangling, time_series_utils
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS, RT_SCHED_GCS, SCHED_GCS

route_time_cols = ["schedule_gtfs_dataset_key", 
                   "route_id", "direction_id", "time_period"]

sort_cols = route_time_cols + ["service_date"]

def concatenate_schedule_by_route_direction(
    date_list: list
) -> pd.DataFrame:
    """
    Concatenate schedule data that's been 
    aggregated to route-direction-time_period.
    """
    FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_route_direction_metrics
    
    df = time_series_utils.concatenate_datasets_across_dates(
        RT_SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
        columns = route_time_cols + [
            "avg_scheduled_service_minutes", 
            "avg_stop_miles",
            "n_trips", "frequency", 
            "is_express", "is_rapid",  "is_rail",
            "is_coverage", "is_downtown_local", "is_local",
        ],
    ).sort_values(sort_cols).rename(
        columns = {
            # rename so we understand data source
            "n_trips": "n_scheduled_trips",
        }
    ).reset_index(drop=True)    
    
    return df


def concatenate_segment_speeds_by_route_direction(
    date_list: list
) -> gpd.GeoDataFrame:
    """
    Concatenate segment speeds data that's been 
    aggregated to route-direction-time_period.
    """
    FILE = GTFS_DATA_DICT.stop_segments.route_dir_single_segment
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        FILE,
        date_list,
        data_type = "gdf",
        columns = route_time_cols + [
            "stop_pair", "stop_pair_name",
            "p20_mph", "p50_mph", 
            "p80_mph", "geometry"],
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df


def concatenate_speeds_by_route_direction(
    date_list: list
) -> pd.DataFrame: 
    """
    Concatenate rt vs schedule data that's been 
    aggregated to route-direction-time_period.
    """
    FILE = GTFS_DATA_DICT.rt_stop_times.route_dir_single_summary

    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        FILE,
        date_list,
        data_type = "df",
        columns = route_time_cols + ["speed_mph"],
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df


def concatenate_rt_vs_schedule_by_route_direction(
    date_list: list
) -> pd.DataFrame:
    FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.vp_route_direction_metrics

    df = time_series_utils.concatenate_datasets_across_dates(
        RT_SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
    ).sort_values(sort_cols).reset_index(drop=True)
    
    # We'll add this back in after merging
    # because these would be NaN if it's not in schedule
    drop_cols = [
        "base64_url", "organization_source_record_id",
        "organization_name", "caltrans_district"
    ]
    
    df = df.drop(columns = drop_cols)
    
    return df


def concatenate_crosswalk_organization(
    date_list: list
) -> pd.DataFrame:
    FILE = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
    ).drop(columns = "itp_id")
    
    return df


def merge_in_standardized_route_names(
    df: pd.DataFrame, 
) -> pd.DataFrame:
    
    keep_cols = [
        "schedule_gtfs_dataset_key", "name", 
        "route_id", "service_date", 
    ]
    
    CLEAN_ROUTES = GTFS_DATA_DICT.schedule_tables.route_identification
    
    route_names_df = pd.read_parquet(
        f"{SCHED_GCS}{CLEAN_ROUTES}.parquet"
    )
    
    route_names_df = time_series_utils.clean_standardized_route_names(
        route_names_df).drop_duplicates()
    
    if "name" in df.columns:
        df = df.drop(columns = "name")
    
    # Use `route_id` to merge to standardized_route_names
    df2 = pd.merge(
        df,
        route_names_df,
        on = ["schedule_gtfs_dataset_key", 
              "route_id", "service_date"],
        how = "left",
    ).drop_duplicates()
    
    # Clean up
    
    # After merging, we can replace route_id with recent_route_id2 
    drop_cols = ["route_desc", "combined_name", "route_id2"]
    
    df3 = time_series_utils.parse_route_combined_name(df2).drop(
        columns = drop_cols).drop_duplicates().reset_index(drop=True)
    
    return df3


def set_primary_typology(df: pd.DataFrame) -> pd.DataFrame:
    """
    Choose a primary typology, and we'll be more generous if 
    multiple typologies are found.
    """
    subset_cols = [c for c in df.columns if "is_" in c and 
                   c not in ["is_ontime", "is_early", "is_late"]]
    keep_cols = route_time_cols + subset_cols
    
    df2 = df[keep_cols].sort_values(
        route_time_cols + subset_cols
    ).drop_duplicates(subset=route_time_cols)
    
    ranks = {
        "coverage": 1,
        "local": 2, 
        "downtown_local": 3,
        "express": 4,        
        "rapid": 5,
        "rail": 6,
    }
    
    # Find the max "score" / typology type, and use that
    for c in ranks.keys():
        df2[f"{c}_score"] = df2[f"is_{c}"] * ranks[c]
    
    df2["max_score"] = df2[[c for c in df2.columns if "_score" in c]].max(axis=1)
    df2["typology"] = df2.max_score.map({v: k for k, v in ranks.items()})
    df2 = df2.assign(
        typology = df2.typology.fillna("unknown")
    )
    
    df3 = df2[route_time_cols + ["typology"]]
    
    return df3

if __name__ == "__main__":
    
    from shared_utils import rt_dates
    
    analysis_date_list = rt_dates.y2024_dates + rt_dates.y2023_dates 
    
    DIGEST_RT_SCHED = GTFS_DATA_DICT.digest_tables.route_schedule_vp 
    DIGEST_SEGMENT_SPEEDS = GTFS_DATA_DICT.digest_tables.route_segment_speeds
    
    df_sched = concatenate_schedule_by_route_direction(analysis_date_list)
    
    primary_typology = set_primary_typology(df_sched)
    
    df_sched2 = pd.merge(
        df_sched,
        primary_typology,
        on = route_time_cols,
        how = "left"
    )

    df_avg_speeds = concatenate_speeds_by_route_direction(analysis_date_list)
                    
    df_rt_sched = (
        concatenate_rt_vs_schedule_by_route_direction(
            analysis_date_list)
        .astype({"direction_id": "float"})
    )
    
    df_crosswalk = concatenate_crosswalk_organization(analysis_date_list)
    
    df = pd.merge(
        df_sched2,
        df_rt_sched,
        on = route_time_cols + ["service_date"],
        how = "outer",
        indicator = "sched_rt_category"
    ).merge(
        df_avg_speeds,
        on = route_time_cols + ["service_date"],
        how = "outer",
    )
    
    df = df.assign(
        sched_rt_category = df.sched_rt_category.map(
            gtfs_schedule_wrangling.sched_rt_category_dict)
    ).pipe(
        merge_in_standardized_route_names,
    ).merge(
        df_crosswalk,
        on = ["schedule_gtfs_dataset_key", "name", "service_date"],
        how = "left"
    )
    
    integrify = [
        "n_scheduled_trips", "n_vp_trips",
        "minutes_atleast1_vp", "minutes_atleast2_vp",
        "total_vp", "vp_in_shape",
        "is_early", "is_ontime", "is_late"
    ]
    
    df[integrify] = df[integrify].fillna(0).astype("int")
    
    df.to_parquet(
        f"{RT_SCHED_GCS}{DIGEST_RT_SCHED}.parquet"
    )
    
    segment_speeds = concatenate_segment_speeds_by_route_direction(
        analysis_date_list
    ).pipe(
        merge_in_standardized_route_names, 
    ).astype({"direction_id": "int64"}) #Int64 doesn't work for gdf
    
    segment_speeds2 = pd.merge(
        segment_speeds,
        primary_typology,
        on = route_time_cols,
        how = "left"
    )
    
    utils.geoparquet_gcs_export(
        segment_speeds2,
        RT_SCHED_GCS,
        DIGEST_SEGMENT_SPEEDS
    )
    
    
