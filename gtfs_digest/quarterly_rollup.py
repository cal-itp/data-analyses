"""
Quarterly Rollup Functions
"""
import pandas as pd
from segment_speed_utils import segment_calcs, metrics
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS
from shared_utils import time_helpers

schd_metric_cols = [
    "avg_scheduled_service_minutes",
    "avg_stop_miles",
    "frequency",
    "total_scheduled_service_minutes",
]

groupby_cols = [
    "schedule_gtfs_dataset_key",
    "year_quarter",
    "direction_id",
    "time_period",
    "route_id",
]
rt_metric_cols = [
    "minutes_atleast1_vp",
    "minutes_atleast2_vp",
    "total_rt_service_minutes",
    "total_vp",
    "vp_in_shape",
    "avg_rt_service_minutes",
    "speed_mph",
]
rt_metric_no_weighted_avg = [
    "is_early",
    "is_ontime",
    "is_late",
]
crosswalk_cols = [
    "base64_url",
    "organization_source_record_id",
    "organization_name",
    "caltrans_district",
    "route_primary_direction",
    "name",
    "schedule_source_record_id",
    "is_express",
    "is_rapid",
    "is_rail",
    "is_coverage",
    "is_downtown_local",
    "is_local",
    "service_date",
    "typology",
    "sched_rt_category",
    "route_long_name",
    "route_short_name",
    "route_combined_name",
    'year', 
    'quarter'
]
group_cols = [
    "year_quarter",
    "schedule_gtfs_dataset_key",
    "route_id",
    "direction_id",
    "time_period",
]

def quarterly_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    GTFS Digest is presented on a monthly candece.
    Aggregate this dataframe to be on a quarterly grain
    instead. 
    """
    # Create quarters
    # Turn date to quarters
    df = time_helpers.add_quarter(df, 'service_date')
    
    # Remove underscore
    df.year_quarter = df.year_quarter.str.replace("_", " ")
    
    # Create copies of the original df before aggregating because I noticed applying
    #  segment_calcs.calculate_weighted_averages impacts the original df
    rt_df = df.copy()
    schd_df = df.copy()
    timeliness_df = df.copy()

    # Calculate RT Metrics that need to have a weighted average
    rt_metrics = segment_calcs.calculate_weighted_averages(
        df=rt_df,
        group_cols=groupby_cols,
        metric_cols=rt_metric_cols,
        weight_col="n_vp_trips",
    )

    # Calculate Scheduled Metrics that need to have a weighted average
    schd_metrics = segment_calcs.calculate_weighted_averages(
        df=schd_df,
        group_cols=groupby_cols,
        metric_cols=schd_metric_cols,
        weight_col="n_scheduled_trips",
    )
    

    # Calculate trips by timeliness which doesn't need weighted average
    timeliness_df = timeliness_df[groupby_cols + rt_metric_no_weighted_avg]
    timeliness_df2 = (
        timeliness_df.groupby(groupby_cols)
        .agg({"is_early": "sum", "is_ontime": "sum", "is_late": "sum"})
        .reset_index()
    )

    # Create a crosswalk with string descriptives such as
    # organization_name, route_long_name, etc that were excluded from the groupby_cols
    crosswalk = df[groupby_cols + crosswalk_cols]

    # Merge all the dataframes
    m1 = (
        pd.merge(rt_metrics, schd_metrics, on=groupby_cols)
        .merge(timeliness_df2, on=groupby_cols)
        .merge(crosswalk, on=groupby_cols)
    )

    # Re-calculate certain columns
    # Have to temporarily rm total to some of the columns
    m1 = m1.rename(
    columns={
        "total_rt_service_minutes": "rt_service_minutes",
        "total_scheduled_service_minutes": "scheduled_service_minutes",
    }
    )
    m1 = metrics.calculate_rt_vs_schedule_metrics(m1)
    
    # Rename back
    m1 = m1.rename(
    columns={
        "rt_service_minutes": "total_rt_service_minutes",
        "scheduled_service_minutes": "total_scheduled_service_minutes"
    }
    ) 
    
    
    # Have to recalculate rt sched journey ratio
    m1["rt_sched_journey_ratio"] = (
        m1.total_rt_service_minutes / m1.total_scheduled_service_minutes
    )
    
    # Rearrange columns to match original df
    col_proper_order = list(df.columns) 
    m1 = m1[col_proper_order]
    
    # Drop service_date & duplicates
    m1 = (m1
          .drop(columns=["service_date"])
          .drop_duplicates(subset = group_cols)
          .reset_index(drop=True))
    return m1

if __name__ == "__main__":
    
    DIGEST_RT_SCHED_MONTH = GTFS_DATA_DICT.digest_tables.monthly_route_schedule_vp 
    DIGEST_RT_SCHED_QTR = GTFS_DATA_DICT.digest_tables.quarterly_route_schedule_vp 
    
    # Save metrics on a monthly candence.
    monthly_df = pd.read_parquet(
        f"{RT_SCHED_GCS}{DIGEST_RT_SCHED_MONTH}.parquet"
    )
    
    # Roll up metrics to be quarterly & Save
    quarter_df = quarterly_metrics(monthly_df)
    quarter_df.to_parquet(
        f"{RT_SCHED_GCS}{DIGEST_RT_SCHED_QTR}.parquet"
    )
    print("Saved GTFS digest")