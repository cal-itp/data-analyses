"""
Merge scheduled stop times with 
what was derived from RT vehicle positions.
"""
import datetime
import pandas as pd

from segment_speed_utils import helpers, segment_calcs
from update_vars import SEGMENT_GCS, RT_SCHED_GCS

def prep_scheduled_stop_times(
    analysis_date: str
) -> pd.DataFrame: 
    """
    Import scheduled stop times and merge in 
    gtfs_dataset_key and trip_instance_key.
    """
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "gtfs_dataset_key",
                   "trip_id", "trip_instance_key"],
        get_pandas = True
    )

    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "trip_id", 
                   "stop_id", "stop_sequence",
                   "arrival_sec",
                  ],
        get_pandas = True,
        with_direction = False
    ).merge(
        trips,
        on = ["feed_key", "trip_id"],
        how = "inner"
    ).drop(
        columns = ["feed_key"]
    ).rename(
        columns = {"arrival_sec": "scheduled_arrival_sec"}
    )
    
    return stop_times


def prep_rt_stop_times(
    analysis_date: str
) -> pd.DataFrame: 
    """
    For RT stop arrivals, drop duplicates based on interpolated
    arrival times. Keep the first arrival time,
    the rest would violate a monotonically increasing condition.
    """
    STOP_ARRIVALS = GTFS_DATA_DICT.rt_stop_times.stage3
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS}_{analysis_date}.parquet",
        columns = ["trip_instance_key", "stop_sequence", "stop_id",
                  "arrival_time"]
    ).rename(columns = {"arrival_time": "rt_arrival"})

    df2 = df.sort_values(
        ["trip_instance_key", "stop_sequence"]
    ).drop_duplicates(
        subset=["trip_instance_key", "rt_arrival"]
    ).reset_index(drop=True)
    
    df2 = segment_calcs.convert_timestamp_to_seconds(
        df2, ["rt_arrival"]
    ).drop(columns = "rt_arrival")
    
    return df2


def assemble_scheduled_rt_stop_times(
    analysis_date: str
) -> pd.DataFrame: 
    """
    Merge scheduled and rt stop times so we can compare
    scheduled arrival (seconds) and RT arrival (seconds).
    """
    sched_stop_times = prep_scheduled_stop_times(analysis_date)
    rt_stop_times = prep_rt_stop_times(analysis_date)
    
    df = pd.merge(
        sched_stop_times,
        rt_stop_times,
        on = ["trip_instance_key", "stop_sequence", "stop_id"],
        how = "inner"
    )
    
    return df


if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    EXPORT_FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.schedule_rt_stop_times
    
    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()
        
        df = assemble_scheduled_rt_stop_times(analysis_date)
        
        df.to_parquet(f"{RT_SCHED_GCS}{EXPORT_FILE}_{analysis_date}.parquet")
        
        end = datetime.datetime.now()
        print(f"execution time: {analysis_date} {end - start}")