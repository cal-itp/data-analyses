"""
Merge scheduled stop times with 
what was derived from RT vehicle positions.
"""
import datetime
import pandas as pd

from segment_speed_utils import array_utils, helpers, segment_calcs
from segment_speed_utils.project_vars import SEGMENT_GCS, RT_SCHED_GCS


def assemble_stop_times(analysis_date: str):
    """
    Import schedule stop times and merge with trips to 
    get trip_instance_key.
    
    Only keep rows where arrival_sec is not NaN.
    """
    sched_trip_cols = ["feed_key", "trip_id"]
    
    # Get trip_instance_key and gtfs_dataset_key
    # and merge to stop_times
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", 
                   "trip_instance_key"] + sched_trip_cols,
        get_pandas = True
    )
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = sched_trip_cols + ["stop_sequence", "arrival_sec"]
    ).merge(
        trips,
        on = sched_trip_cols, 
        how = "inner"
    ).drop(columns = ["feed_key"]).compute()
    
    stop_times2 = stop_times[
        stop_times.arrival_sec.notna()
    ].reset_index(drop=True).astype({"arrival_sec": "int"})
    
    return stop_times2


def main(analysis_date: str):
    # Import scheduled stop times
    stop_times = assemble_stop_times(analysis_date).rename(
        columns = {"arrival_sec": "scheduled_arrival_sec"})
    
    # Import stop arrivals (which has interpolated arrival time)
    stop_arrivals = pd.read_parquet(
        f"{SEGMENT_GCS}stop_arrivals_{analysis_date}.parquet",
        columns = ["trip_instance_key", "stop_sequence", "arrival_time"]
    ).rename(columns = {"arrival_time": "actual_arrival"})

    trips_with_rt = stop_arrivals.trip_instance_key.unique()
    
    stop_arrivals = segment_calcs.convert_timestamp_to_seconds(
        stop_arrivals, ["actual_arrival"]).drop(columns = "actual_arrival")
    
    # Check a rolling window, flag those fail monotonic test
    stop_arrivals2 = array_utils.rolling_window_make_array(
        stop_arrivals, 
        window = 3, rolling_col = "actual_arrival_sec"
    )
    
    stop_arrivals3 = stop_arrivals2[
        stop_arrivals2.actual_arrival_sec_monotonic==True
    ].drop(columns = ["rolling_actual_arrival_sec", 
                      "actual_arrival_sec_monotonic"])
    
    
    # Merge scheduled with RT stop_times, only keep rows with vp
    df = pd.merge(
        stop_times[stop_times.trip_instance_key.isin(trips_with_rt)],
        stop_arrivals3,
        on = ["trip_instance_key", "stop_sequence"],
        how = "left"
    )
    
    df.to_parquet(f"{RT_SCHED_GCS}rt_stop_times_{analysis_date}.parquet")
    
    return 


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()
        
        main(analysis_date)
        
        end = datetime.datetime.now()
        print(f"execution time: {analysis_date} {end - start}")