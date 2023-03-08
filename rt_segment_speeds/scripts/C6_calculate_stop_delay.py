"""
Calculate stop delay.

Merge scheduled stop times table with 
RT stop-to-stop segment speeds, and use that
to calculate the difference in actual vs scheduled arrival.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from shared_utils import utils
from segment_speed_utils import gtfs_schedule_wrangling, helpers, segment_calcs
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH)
                                              
    
def import_segment_speeds_and_localize_timestamp(
    analysis_date: str, 
    dict_inputs: dict = {}
) -> dd.DataFrame:
    """
    Import speeds by stop segments.
    Localize the max_time (which should be the time closest to the stop_id) 
    from UTC and then convert it to seconds.
    """
    SPEED_FILE = dict_inputs["stage4"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    
    speeds = dd.read_parquet(
        f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}/",
        columns = [
            "gtfs_dataset_key", "_gtfs_dataset_name",
            "trip_id"] + SEGMENT_IDENTIFIER_COLS + [
            "max_time", "speed_mph"]
    )
    
    speeds_local_time = segment_calcs.localize_vp_timestamp(
        speeds, ["max_time"])
    
    speeds_local_time = segment_calcs.convert_timestamp_to_seconds(
        speeds_local_time, "max_time")
        
    return speeds_local_time
    
    
def calculate_delay_for_stop_segments(
    analysis_date: str,
    dict_inputs: dict = {}
) -> dd.DataFrame:
    """
    Merge scheduled stop times with stop-to-stop segment speeds / times 
    and calculate stop delay.
    """
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    
    rt_speeds = import_segment_speeds_and_localize_timestamp(
        analysis_date, dict_inputs)
    
    trips = helpers.import_scheduled_trips(
        analysis_date, 
        columns = ["feed_key", "name", "trip_id", GROUPING_COL]
    ).compute()
             
    trips = gtfs_schedule_wrangling.exclude_scheduled_operators(trips)
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date, 
        columns = [
            "feed_key", "trip_id", 
            "stop_id", "stop_sequence",
            "arrival_sec"
        ]
    )
    
    scheduled_stop_times = gtfs_schedule_wrangling.merge_shapes_to_stop_times(
        stop_times, trips) 
    
    # Merge scheduled and RT stop times
    df = dd.merge(
        rt_speeds,
        scheduled_stop_times,
        on = SEGMENT_IDENTIFIER_COLS + ["trip_id"],
        how = "inner"
    )
    
    # Calculate difference between RT arrival time closest to stop
    # and scheduled arrival time at stop
    df_with_delay = segment_calcs.derive_stop_delay(
        df, 
        ("max_time_sec", "arrival_sec")
    )
   
    return df_with_delay
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/C6_calculate_stop_delay.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    speed_delay_by_stop_segment = calculate_delay_for_stop_segments(
        analysis_date, 
        STOP_SEG_DICT
    )
    
    time1 = datetime.datetime.now()
    logger.info("merge scheduled and RT stop times and "
                f"calculate delay: {time1 - start}")
    
    df = speed_delay_by_stop_segment.compute()
    
    df.to_parquet(
        f"{SEGMENT_GCS}stop_segments_with_speed_delay_{analysis_date}.parquet")

    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
