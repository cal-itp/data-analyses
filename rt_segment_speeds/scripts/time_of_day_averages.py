"""
Cache a new time-of-day single day grain.
Use that to build all the other aggregations
for multiday. 

TODO: refactor for weighted averages and hopefully
it's faster to pull multiple days with these cached outputs.
can we weight it by number of trips and use vectorized operations?

TODO: make clear year/quarter or weekday/weekend grain
"""
import datetime
import pandas as pd
import sys

from loguru import logger
from typing import Literal

from segment_speed_utils import segment_calcs
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              GTFS_DATA_DICT, 
                                              SEGMENT_TYPES)

def aggregate_by_time_of_day(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES]
):
    """
    Set the time-of-day single day aggregation
    and calculate 20th/50th/80th percentile speeds.
    These daily metrics feed into multi-day metrics.
    """
    start = datetime.datetime.now()
    
    dict_inputs = GTFS_DATA_DICT[segment_type]
        
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    EXPORT_FILE = dict_inputs["segment_timeofday"]
    
    SEGMENT_COLS = [*dict_inputs["segment_cols"]]
    SEGMENT_COLS = [i for i in SEGMENT_COLS if i != "geometry"]
                                      
    OPERATOR_COLS = ["schedule_gtfs_dataset_key"]
     
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}.parquet",
        columns = SEGMENT_COLS + ["trip_instance_key", "time_of_day", "speed_mph"],
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    ).dropna(subset="speed_mph").pipe(
        segment_calcs.calculate_avg_speeds,
        SEGMENT_COLS + ["time_of_day"]
    )
                    
    df.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
    )
                    
    end = datetime.datetime.now()
    logger.info(
        f"{segment_type}: time-of-day averages for {analysis_date} "
        f"execution time: {end - start}"
    )
    
    
    return

def weighted_average_across_multiple_days(
    analysis_date_list: list
): 
    """
    can we use dask_utils to put together
    several days, weight the speed by n_trips,
    and roll it up further?
    """
    
    return



if __name__ == "__main__":

    from segment_speed_utils.project_vars import analysis_date_list
    LOG_FILE = "../logs/test.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    segment_type = "stop_segments"
                        
    for analysis_date in analysis_date_list:
        aggregate_by_time_of_day(analysis_date, segment_type)