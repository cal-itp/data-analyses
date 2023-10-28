"""
Quick script to back out the stop arrivals, even if partial,
to speeds.
"""
import datetime
import pandas as pd

from shared_utils import rt_dates
from segment_speed_utils import segment_calcs
from segment_speed_utils.project_vars import SEGMENT_GCS

if __name__ == "__main__":
    
    analysis_date = rt_dates.DATES["sep2023"]
    
    print(f"Analysis date: {analysis_date}")    
    
    start = datetime.datetime.now()
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}stop_arrivals_{analysis_date}_2.parquet"
    )
    
    trip_stop_cols = ["trip_instance_key", "stop_sequence"]

    df = segment_calcs.convert_timestamp_to_seconds(
        df, ["arrival_time"]
    ).sort_values(trip_stop_cols).reset_index(drop=True)
    
    df = df.assign(
        prior_arrival_time_sec = (df.groupby("trip_instance_key")
                                  .arrival_time_sec
                                  .shift(1)
                                 ),
        prior_stop_meters = (df.groupby("trip_instance_key")
                             .stop_meters
                             .shift(1)
                            )
    )

    speed = df.assign(
        meters_elapsed = df.stop_meters - df.prior_stop_meters, 
        sec_elapsed = df.arrival_time_sec - df.prior_arrival_time_sec,
    ).pipe(
        segment_calcs.derive_speed, 
        ("prior_stop_meters", "stop_meters"), 
        ("prior_arrival_time_sec", "arrival_time_sec")
    )
    
    speed.to_parquet(
        f"{SEGMENT_GCS}stop_arrivals_speed_{analysis_date}_2.parquet")
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")