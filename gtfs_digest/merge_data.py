"""
Merge datasets across dates to create time-series data
for GTFS digest.
Grain is operator-service_date-route-direction-time_period.
"""
import geopandas as gpd
import pandas as pd
import numpy as np

from segment_speed_utils import gtfs_schedule_wrangling, time_series_utils
from shared_utils import gtfs_utils_v2, publish_utils
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS, RT_SCHED_GCS, SCHED_GCS

route_time_cols = [
    "schedule_gtfs_dataset_key", 
    "route_id", "direction_id", 
     "time_period"
]

sort_cols = route_time_cols + ["service_date"]

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
    
    route_time_dir_cols = route_time_cols + ["route_primary_direction"]
    
    df = time_series_utils.concatenate_datasets_across_dates(
        RT_SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
        columns = route_time_dir_cols + [
            "avg_scheduled_service_minutes", 
            "avg_stop_miles",
            "n_trips", "frequency", 
            "is_express", "is_rapid",  "is_rail",
            "is_coverage", "is_downtown_local", "is_local",
        ],
    ).sort_values(route_time_dir_cols + ["service_date"]).rename(
        columns = {
            # rename so we understand data source
            "n_trips": "n_scheduled_trips",
        }
    ).reset_index(drop=True)    
    
    return df


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
    ).sort_values(sort_cols).reset_index(drop=True)
    
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
    )[route_time_cols + ["typology"]]
    
    
    return df2
"""
Quarterly Rollup Functions
"""
def quarterly_rollup(df:pd.DataFrame, metric_columns:list)->pd.DataFrame:
    """
    roll up months to each quarter for certain metrics.
    """
    quarterly_metrics = segment_calcs.calculate_weighted_averages(
    
    df=df,
    group_cols=[
        "quarter",
        "Period",
        "Organization",
        "Route",
        "dir_0_1",
        "Direction",
    ],
    metric_cols= metric_columns,
    weight_col="# Trips with VP",
    )
    return quarterly_metrics

def rollup_schd_qtr(peak_offpeak_df:pd.DataFrame)->pd.DataFrame:
    """
    Roll up # Scheduled Trips to be on a quarterly basis
    since this metric doesn't change very often. 
    """
    # Aggregate
    agg1 = (
    peak_offpeak_df.groupby(
        ["quarter", "Period", "Organization", "Route", "dir_0_1", "Direction"]
    )
    .agg({"Date":"nunique","# scheduled trips": "sum"})
    .reset_index()
    )
    
    # If a quarter is complete with all 3 months, divide by 3
    agg1.loc[agg1["Date"] == 3, "# scheduled trips"] = (
    agg1.loc[agg1["Date"] == 3, "# scheduled trips"] / 3)
    
    # If a quarter is incomplete with only 2 months, divide by 2 
    agg1.loc[agg1["Date"] == 2, "# scheduled trips"] = (
    agg1.loc[agg1["Date"] == 2, "# scheduled trips"] / 2
)
    return agg1
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
    # Get primary route type
    primary_typology = set_primary_typology(df_schedule)
    
    df_schedule2 = pd.merge(
        df_schedule,
        primary_typology,
        on = route_time_cols,
        how = "left"
    )
    
    df = pd.merge(
        df_schedule2,
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
        on = ["schedule_gtfs_dataset_key", "service_date"],
        how = "left"
    ).pipe(
        # Find the most common cardinal direction
        gtfs_schedule_wrangling.top_cardinal_direction
    )
     
    integrify = [
        "n_scheduled_trips", "n_vp_trips",
        "minutes_atleast1_vp", "minutes_atleast2_vp",
        "total_vp", "vp_in_shape",
        "is_early", "is_ontime", "is_late"
    ]
    
    df[integrify] = df[integrify].fillna(0).astype("int")
    
    # Clean up repeated columns
    df["name"] = df.name_x.fillna(df.name_y)
    df["schedule_source_record_id"] = df.schedule_source_record_id_x.fillna(df.schedule_source_record_id_y)
    df = df.drop(columns = ["name_x",
                           "name_y",
                           "schedule_source_record_id_x",
                           "schedule_source_record_id_y"])
    return df


if __name__ == "__main__":
    
    from shared_utils import rt_dates
    
    analysis_date_list = (
        rt_dates.y2024_dates + rt_dates.y2023_dates +
        rt_dates.y2025_dates
    )
    
    DIGEST_RT_SCHED = GTFS_DATA_DICT.digest_tables.route_schedule_vp 
    DIGEST_SEGMENT_SPEEDS = GTFS_DATA_DICT.digest_tables.route_segment_speeds
    
    # These are public schedule_gtfs_dataset_keys
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    # Get cardinal direction for each route
    df_sched = concatenate_schedule_by_route_direction(analysis_date_list).pipe(
        # Drop any private datasets before exporting
        publish_utils.exclude_private_datasets, 
        public_gtfs_dataset_keys = public_feeds
    )
    df_avg_speeds = concatenate_speeds_by_route_direction(
        analysis_date_list
    ).pipe(
        publish_utils.exclude_private_datasets, 
        public_gtfs_dataset_keys = public_feeds
    )
    df_rt_sched = (
        concatenate_rt_vs_schedule_by_route_direction(
            analysis_date_list
        ).pipe(
            publish_utils.exclude_private_datasets, 
            public_gtfs_dataset_keys = public_feeds
        ).astype({"direction_id": "float"})
    )
    
    df_crosswalk = concatenate_crosswalk_organization(
        analysis_date_list
    ).pipe(
        publish_utils.exclude_private_datasets, 
        public_gtfs_dataset_keys = public_feeds
    )
    
    df = merge_data_sources_by_route_direction(
        df_sched,
        df_rt_sched,
        df_avg_speeds,
        df_crosswalk
    )
    
    # Save metrics on a monthly candence.
    df.to_parquet(
        f"{RT_SCHED_GCS}{DIGEST_RT_SCHED}.parquet"
    )
   
    # Roll up monthly metrics to quarterly
    # Filter for only rows that are "all day" statistics
    all_day = df.loc[df["Period"] == "all_day"].reset_index(drop=True)
    
    # Filter for only rows that are "peak/offpeak" statistics
    peak_offpeak_df = df.loc[df["Period"] != "all_day"].reset_index(drop=True)
    
    # Roll up some metrics that don't change too much
    # to be quarterly instead of monthly
    quarter_rollup_all_day = quarterly_rollup(all_day, [
        "Average VP per Minute",
        "% VP within Scheduled Shape",
        "Average Scheduled Service (trip minutes)",
        "ruler_100_pct",
        "ruler_for_vp_per_min"
    ]) 
    
    total_scheduled_trips = rollup_schd_qtr(peak_offpeak_df)
    
    # Merge these 2 
    m1 = pd.merge(quarter_rollup_all_day, total_scheduled_trips)
    print("Saved Digest RT")
    
