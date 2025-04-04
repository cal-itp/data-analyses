"""
Merge datasets across dates to create time-series data
for GTFS digest.
Grain is operator-service_date-route-direction-time_period.
"""
import pandas as pd

from segment_speed_utils import gtfs_schedule_wrangling, time_series_utils
from shared_utils import dask_utils, gtfs_utils_v2, portfolio_utils, publish_utils
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS, RT_SCHED_GCS, SCHED_GCS
from shared_utils import rt_dates

route_time_cols = [
    "schedule_gtfs_dataset_key", 
    "route_id", "direction_id", 
    "time_period"
]

route_time_date_cols = route_time_cols + ["service_date"]

"""
Concatenating Functions 
"""
def concatenate_schedule_by_route_direction(
    date_list: list
) -> pd.DataFrame:
    """
    Concatenate schedule metrics (from gtfs_funnel)
    for route-direction-time_period grain
    for all the dates we have.
    """
    FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_route_direction_metrics
    ROUTE_TYPOLOGIES_FILE = GTFS_DATA_DICT.schedule_tables.route_typologies

    df = time_series_utils.concatenate_datasets_across_dates(
        RT_SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
        columns = route_time_cols + [
            "avg_scheduled_service_minutes", 
            "avg_stop_miles",
            "route_primary_direction",
            "n_trips", "frequency", 
        ],
    ).sort_values(route_time_date_cols).rename(
        columns = {
            # rename so we understand data source
            "n_trips": "n_scheduled_trips",
        }
    ).reset_index(drop=True)   
    
    # Create a year column to use for merging with route typologies
    df = df.assign(
        year = df.service_date.dt.year
    )
    
    route_typology_paths = [
        f"{SCHED_GCS}{ROUTE_TYPOLOGIES_FILE}" 
        for year in rt_dates.years_available
    ]

    route_typology_df = dask_utils.get_ddf(
        route_typology_paths, 
        rt_dates.years_available, 
        data_type = "df",
        get_pandas = True,
        columns = [
            "schedule_gtfs_dataset_key", "route_id", 
            "is_express", "is_ferry", "is_rail",
            "is_coverage", "is_local", "is_downtown_local", "is_rapid"
        ],
        add_date = False, 
        add_year = True
    )  
        
    df2 = pd.merge(
        df,
        route_typology_df,
        on = ["schedule_gtfs_dataset_key", "route_id", "year"],
        how = "left" 
    ).drop(
        columns = "year"
    ).pipe(
        set_primary_typology
    ).pipe(
        merge_in_standardized_route_names
    )
    
    # TODO: double check it's for route-direction across dates
    route_cols = ["schedule_gtfs_dataset_key", 
                  "recent_combined_name", "direction_id"]
    
    top_cardinal_direction = gtfs_schedule_wrangling.mode_by_group(
        df2,
        group_cols = route_cols,
        value_cols = ["route_primary_direction"]
    )
    
    df3 = pd.merge(
        df2.drop(columns = ["route_primary_direction"]),
        top_cardinal_direction,
        on = route_cols,
        how = "inner"
    )
    
    return df3


def concatenate_speeds_by_route_direction(
    date_list: list
) -> pd.DataFrame: 
    """
    Concatenate summary speeds (from rt_segment_speeds) 
    for route-direction-time_period grain 
    for all the dates we have.
    """
    FILE = GTFS_DATA_DICT.rt_stop_times.route_dir_timeofday

    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        FILE,
        date_list,
        data_type = "df",
        columns = route_time_cols + ["speed_mph"],
    ).sort_values(route_time_date_cols).reset_index(drop=True)
    
    return df


def concatenate_rt_vs_schedule_by_route_direction(
    date_list: list
) -> pd.DataFrame:
    """
    Concatenate the RT metrics (from rt_vs_schedule) 
    for route-direction_time_period grain 
    for all the dates we have.
    """
    FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.vp_route_direction_metrics

    df = time_series_utils.concatenate_datasets_across_dates(
        RT_SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
    ).sort_values(
        route_time_date_cols
    ).reset_index(drop=True).astype(
        {"direction_id": "float"}
    )
    
    # We'll add this back in after merging
    # because these would be NaN if it's not in schedule
    drop_cols = [
        "base64_url", "organization_source_record_id",
        "organization_name", "caltrans_district",
        "schedule_source_record_id", "name"
    ]
    
    df = df.drop(columns = drop_cols)
    
    return df


def concatenate_crosswalk_organization(
    date_list: list
) -> pd.DataFrame:
    """
    Concatenate the crosswalk (from gtfs_funnel) 
    that connects gtfs_dataset_key to organization
    and other organization-related columns (NTD, etc)
    for all the dates we have.
    
    This is operator grain.
    """
    FILE = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk
    
    crosswalk_cols = [
        "schedule_gtfs_dataset_key",
        "name",
        "schedule_source_record_id",
        "base64_url",
        "organization_source_record_id",
        "organization_name",
        "caltrans_district"
    ]
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
        columns = crosswalk_cols
    )
    
    df = df.assign(
        caltrans_district = df.caltrans_district.map(
            portfolio_utils.CALTRANS_DISTRICT_DICT
        )
    )
    
    return df

"""
Rectifying Functions
"""
def merge_in_standardized_route_names(
    df: pd.DataFrame, 
) -> pd.DataFrame:
    """
    Use the clean route names file that standardizes route names once.
    and re-parse it for parts to the name.
    """
    keep_cols = [
        "schedule_gtfs_dataset_key", "name", 
        "route_id", "service_date", 
    ]
    
    CLEAN_ROUTES = GTFS_DATA_DICT.schedule_tables.route_identification
    
    route_names_df = pd.read_parquet(
        f"{SCHED_GCS}{CLEAN_ROUTES}.parquet"
    ).pipe(time_series_utils.clean_standardized_route_names).drop_duplicates()
    
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
    
    
    # After merging, only route_id reflects that original scheduled trips column
    # the other recent_combined_name, recent_route_id reflect the last observed value
    drop_cols = [
        "route_id2", 
        "route_short_name", "route_long_name",
        "route_desc"
    ]
    
    df3 = time_series_utils.parse_route_combined_name(
        df2
    ).drop(
        columns = drop_cols
    )
    
    return df3


def set_primary_typology(df: pd.DataFrame) -> pd.DataFrame:
    """
    Choose a primary typology, and we'll be more generous if 
    multiple typologies are found.
    """ 
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
        df[f"{c}_score"] = df[f"is_{c}"] * ranks[c]
    
    df["max_score"] = df[[c for c in df.columns if "_score" in c]].max(axis=1)
    df["typology"] = df.max_score.map({v: k for k, v in ranks.items()})
    
    drop_cols = [c for c in df.columns if "_score" in c]
    
    df2 = df.assign(
        typology = df.typology.fillna("unknown")
    ).drop(columns = drop_cols)
    
    return df2



"""
Merging Functions
"""
def merge_data_sources_by_route_direction(
    df_schedule: pd.DataFrame,
    df_rt_sched: pd.DataFrame,
    df_avg_speeds: pd.DataFrame,
    df_crosswalk: pd.DataFrame
):
    """
    Merge schedule, rt_vs_schedule, and speeds data, 
    which are all at route-direction-time_period-date grain.
    This merged dataset will be used in GTFS digest visualizations.
    """    
    df = pd.merge(
        df_schedule,
        df_rt_sched,
        on = route_time_date_cols,
        how = "outer",
        indicator = "sched_rt_category"
    ).merge(
        df_avg_speeds,
        on = route_time_date_cols,
        how = "outer",
    )
    
    df = df.assign(
        sched_rt_category = df.sched_rt_category.map(
            gtfs_schedule_wrangling.sched_rt_category_dict
        )
    ).merge(
        df_crosswalk,
        on = ["schedule_gtfs_dataset_key", "name", "service_date"],
        how = "left"
    )
     
    integrify = [
        "n_scheduled_trips", "n_vp_trips",
        "minutes_atleast1_vp", "minutes_atleast2_vp",
        "total_vp", "vp_in_shape",
        "is_early", "is_ontime", "is_late",
        # fillna only before visualizing, doing it before prevents the merges from succeeding
        "direction_id"
    ]
    
    df[integrify] = df[integrify].fillna(0).astype("int")
    
    return df


if __name__ == "__main__":
    
    from shared_utils import rt_dates
    
    analysis_date_list = (
        rt_dates.y2024_dates + rt_dates.y2023_dates +
        rt_dates.y2025_dates
    )
    
    DIGEST_RT_SCHED = GTFS_DATA_DICT.digest_tables.route_schedule_vp 
    
    # These are public schedule_gtfs_dataset_keys
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    df_sched = concatenate_schedule_by_route_direction(analysis_date_list)
    
    df_avg_speeds = concatenate_speeds_by_route_direction(
        analysis_date_list
    )
    
    df_rt_sched = concatenate_rt_vs_schedule_by_route_direction(
        analysis_date_list
    )
    
    df_crosswalk = concatenate_crosswalk_organization(
        analysis_date_list
    )
    
    df = merge_data_sources_by_route_direction(
        df_sched,
        df_rt_sched,
        df_avg_speeds,
        df_crosswalk
    ).pipe(
        publish_utils.exclude_private_datasets, 
        public_gtfs_dataset_keys = public_feeds
    )
    
    # Save metrics on a monthly candence.
    df.to_parquet(
        f"{RT_SCHED_GCS}{DIGEST_RT_SCHED}.parquet"
    )
    print("Saved GTFS digest")
