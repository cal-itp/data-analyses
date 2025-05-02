"""
Use generalized 100 meters, calculate speeds
on the 100 meters segment off of vp path.
"""
import datetime
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT
from shared_utils import rt_dates
from B2_modeled_rt_stop_times_speeds import speed_arrays_by_segment_trip

if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_speeds.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    analysis_date_list = [
        rt_dates.DATES["oct2024"]
    ]    

    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()

        INPUT_FILE = GTFS_DATA_DICT.modeled_vp.resampled_vp
        EXPORT_FILE = GTFS_DATA_DICT.modeled_vp.speeds_wide

        df = pd.read_parquet(
            f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet"
        )

        results = speed_arrays_by_segment_trip(
            df,
            trip_group_cols = ["trip_instance_key"], 
            meters_interval = 100,
        )

        results.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
        )

        end = datetime.datetime.now()
        logger.info(f"{analysis_date}: speeds every 100m: {end - start}")