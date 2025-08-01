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
    "portfolio_organization_name",
    "year_quarter",
    "direction_id",
    "time_period",
    "recent_combined_name",
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
        "caltrans_district",
        "combined_name",
        "is_coverage",
        "is_downtown_local",
        "is_express",
        "is_ferry",
        "is_local",
        "is_rail",
        "is_rapid",
        "name",
        "recent_route_id",
        "route_id",
        "route_primary_direction",
        "sched_rt_category",
        "schedule_gtfs_dataset_key",
        "schedule_source_record_id",
        "typology",
        "portfolio_organization_name",
        "year_quarter",
        "direction_id",
        "time_period",
        "recent_combined_name"
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

    # Calculate RT Metrics that need to have a weighted average
    rt_metrics = segment_calcs.calculate_weighted_averages(
        df=df[groupby_cols+rt_metric_cols+["n_vp_trips"]],
        group_cols=groupby_cols,
        metric_cols=rt_metric_cols,
        weight_col="n_vp_trips",
    )

    # Calculate Scheduled Metrics that need to have a weighted average
    schd_metrics = segment_calcs.calculate_weighted_averages(
        df=df[groupby_cols + schd_metric_cols + ["n_scheduled_trips"]],
        group_cols=groupby_cols,
        metric_cols=schd_metric_cols,
        weight_col="n_scheduled_trips",
    )
    

    # Calculate trips by timeliness which doesn't need weighted average
    timeliness_df = df[groupby_cols + rt_metric_no_weighted_avg]
    timeliness_df2 = (
        timeliness_df.groupby(groupby_cols)
        .agg({"is_early": "sum", "is_ontime": "sum", "is_late": "sum"})
        .reset_index()
    )

    # Create a crosswalk with string descriptives such as
    # organization_name, route_long_name, etc that were excluded from the groupby_cols
    # crosswalk = df[crosswalk_cols].drop_duplicates()
    crosswalk = df[crosswalk_cols].drop_duplicates()
    
    # Merge all the dataframes
    m1 = (
        pd.merge(rt_metrics, schd_metrics, on=groupby_cols)
        .merge(timeliness_df2, on=groupby_cols)
        .merge(crosswalk, on=groupby_cols)
    )

    # Re-calculate certain columns
    # Have to temporarily rm total to some of the columns
    m2 = m1.rename(
    columns={
        "total_rt_service_minutes": "rt_service_minutes",
        "total_scheduled_service_minutes": "scheduled_service_minutes",
    }).pipe(
     metrics.calculate_rt_vs_schedule_metrics
    ).rename(
       columns={
        "rt_service_minutes": "total_rt_service_minutes",
        "scheduled_service_minutes": "total_scheduled_service_minutes"

    })
 
    # Have to recalculate rt sched journey ratio
    m2["rt_sched_journey_ratio"] = (
        m2.total_rt_service_minutes / m2.total_scheduled_service_minutes
    )
    
    # Rearrange columns to match original df
    col_proper_order = list(df.columns) 
    col_proper_order.remove("service_date")
    col_proper_order.remove("year")
    col_proper_order.remove("quarter")
    m2 = m2[col_proper_order]
    
    return m2

if __name__ == "__main__":
    
    DIGEST_RT_SCHED_MONTH = GTFS_DATA_DICT.digest_tables.monthly_route_schedule_vp 
    DIGEST_RT_SCHED_QTR = GTFS_DATA_DICT.digest_tables.quarterly_route_schedule_vp
    
    # Open metrics on a monthly candence.
    monthly_df = pd.read_parquet(
        f"{RT_SCHED_GCS}{DIGEST_RT_SCHED_MONTH}.parquet"
    )
    
    # Roll up metrics to be quarterly & Save
    quarter_df = quarterly_metrics(monthly_df)
    quarter_df.to_parquet(
        f"{RT_SCHED_GCS}{DIGEST_RT_SCHED_QTR}.parquet"
    )
    print("Saved GTFS digest")