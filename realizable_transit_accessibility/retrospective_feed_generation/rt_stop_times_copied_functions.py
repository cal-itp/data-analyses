from segment_speed_utils import helpers, segment_calcs
from calitp_data_analysis.gcs_pandas import GCSPandas
from .constants import GTFS_DATA_DICT
import pandas as pd

SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS
p = GCSPandas()

# Unchanged from rt_scheduled_v_ran, but isn't in a package so we have to copy paste for now
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

# Unchanged from rt_scheduled_v_ran, but isn't in a package so we have to copy paste for now
def prep_rt_stop_times(
    analysis_date: str,
    trip_stop_cols: list
) -> pd.DataFrame: 
    """
    For RT stop arrivals, drop duplicates based on interpolated
    arrival times. Keep the first arrival time,
    the rest would violate a monotonically increasing condition.
    """
    STOP_ARRIVALS = GTFS_DATA_DICT.rt_stop_times.stage3
    
    df = p.read_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS}_{analysis_date}.parquet",
        columns = trip_stop_cols + ["arrival_time"]
    ).rename(columns = {"arrival_time": "rt_arrival"})

    df2 = df.sort_values(
        trip_stop_cols
    ).drop_duplicates(
        subset=["trip_instance_key", "rt_arrival"]
    ).reset_index(drop=True)
    
    df2 = segment_calcs.convert_timestamp_to_seconds(
        df2, ["rt_arrival"]
    ).drop(columns = "rt_arrival")
    
    return df2

def assemble_scheduled_rt_stop_times_keep_all_scheduled(
    analysis_date: str,
    trip_stop_cols: list
) -> pd.DataFrame: 
    """
    Merge scheduled and rt stop times so we can compare
    scheduled arrival (seconds) and RT arrival (seconds).
    Use an outer merge, so stop-times without RT are included.
    """
    sched_stop_times = prep_scheduled_stop_times(analysis_date)
    rt_stop_times = prep_rt_stop_times(analysis_date, trip_stop_cols)
    
    df = pd.merge(
        sched_stop_times,
        rt_stop_times,
        on = trip_stop_cols,
        how = "left"
    )
    return df