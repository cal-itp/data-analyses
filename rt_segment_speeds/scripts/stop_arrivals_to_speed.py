"""
Convert stop-to-stop arrivals into speeds.
"""
import datetime
import pandas as pd
import sys

from loguru import logger

from shared_utils import rt_dates
from segment_speed_utils import helpers, segment_calcs
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH

if __name__ == "__main__":
    
    LOG_FILE = "../logs/speeds_by_segment_trip.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    analysis_date = rt_dates.DATES["sep2023"]
    logger.info(f"Analysis date: {analysis_date}")
    
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    STOP_ARRIVALS_FILE = f"{STOP_SEG_DICT['stage3']}_{analysis_date}"
    SPEED_FILE = f"{STOP_SEG_DICT['stage4']}_{analysis_date}"
        
    start = datetime.datetime.now()
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}stop_arrivals_{analysis_date}.parquet"
    )
    
    trip_stop_cols = ["trip_instance_key", "stop_sequence"]

    df = segment_calcs.convert_timestamp_to_seconds(
        df, ["arrival_time"]
    ).sort_values(trip_stop_cols).reset_index(drop=True)
    
    df = df.assign(
        prior_arrival_time_sec = (df.groupby("trip_instance_key", 
                                             observed=True, group_keys=False)
                                  .arrival_time_sec
                                  .shift(1)
                                 ),
        prior_stop_meters = (df.groupby("trip_instance_key", 
                                        observed=True, group_keys=False)
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
        f"{SEGMENT_GCS}{SPEED_FILE}.parquet")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")