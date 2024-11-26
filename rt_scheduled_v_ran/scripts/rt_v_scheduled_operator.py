"""
Generate RT vs schedule metrics for agency-level.
"""
import datetime
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import gtfs_schedule_wrangling, metrics

from update_vars import RT_SCHED_GCS, GTFS_DATA_DICT
from shared_utils import rt_dates

def operator_metrics(analysis_date: str, dict_inputs: dict) -> pd.DataFrame:
    start = datetime.datetime.now()

    TRIP_EXPORT = dict_inputs.vp_trip_metrics
    OP_EXPORT = dict_inputs.vp_operator_metrics

    # Read in dataframe.
    df = pd.read_parquet(f"{RT_SCHED_GCS}{TRIP_EXPORT}_{analysis_date}.parquet")
  
    # Aggregate
    groupby_cols = [
        "schedule_gtfs_dataset_key",
    ]

    sum_cols = ["total_vp", "vp_in_shape", "rt_service_minutes"]
    agg1 = df.groupby(groupby_cols).agg({**{e: "sum" for e in sum_cols}}).reset_index()

    agg1["vp_per_min_agency"] = ((agg1.total_vp / agg1.rt_service_minutes)).round(2)
    agg1["spatial_accuracy_agency"] = ((agg1.vp_in_shape / agg1.total_vp) * 100).round(2)
    
    # Clean
    agg1 = agg1.drop(columns=sum_cols)
    
    # Save
    agg1.to_parquet(f"{RT_SCHED_GCS}{OP_EXPORT}_{analysis_date}.parquet")

    end = datetime.datetime.now()
    logger.info(f"agency aggregation {analysis_date}: {end - start}")

    return agg1

if __name__ == "__main__":
    
    LOG_FILE = "../logs/rt_v_scheduled_operator_metrics.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    from update_vars import analysis_date_list
    
    dict_inputs = GTFS_DATA_DICT.rt_vs_schedule_tables
    
    for analysis_date in analysis_date_list: 
        operator_metrics(analysis_date, dict_inputs)