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
from segment_speed_utils import sched_rt_utils, segment_calcs
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              COMPILED_CACHED_VIEWS)


def import_speeds_by_stop_segments(analysis_date: str) -> dd.DataFrame:
    """
    Import speeds by stop segments.
    Localize the max_time (which should be the time closest to the stop_id) 
    from UTC and then convert it to seconds.
    """
    speeds = dd.read_parquet(
        f"{SEGMENT_GCS}speed_stops_{analysis_date}/",
        columns = [
            "gtfs_dataset_key", "_gtfs_dataset_name",
            "trip_id", "shape_array_key", "stop_sequence",
            "max_time", "speed_mph"]
    )
    
    speeds_local_time = vp_utils.localize_vp_timestamp(speeds)
    
    return speeds_local_time
  
    
def merge_scheduled_and_rt_stop_times(analysis_date) -> dd.DataFrame:
    """
    Merge scheduled stop times with stop-to-stop segment speeds / times 
    and calculate stop delay.
    """
    rt_speeds = import_speeds_by_stop_segments(analysis_date)
    
    trips = sched_rt_utils.get_scheduled_trips(
        analysis_date, 
        trip_cols = ["feed_key", "name", "trip_id", "shape_array_key"]
    ).compute()
             
    exclude = ["Amtrak Schedule"]
    trips = trips[~trips.name.isin(exclude)].reset_index(drop=True)
    
    stop_times = sched_rt_utils.get_stop_times(
        analysis_date, 
        stop_time_cols = [
            "feed_key", "trip_id", 
            "stop_id", "stop_sequence",
            "arrival_sec"
        ]
    )         
    
    scheduled_stop_times = NEW_stop_time_utils.merge_shapes_to_stop_times(
        stop_times, trips)   
    
    # Merge scheduled and RT stop times
    df = dd.merge(
        rt_speeds,
        scheduled_stop_times,
        on = ["shape_array_key", "trip_id", "stop_sequence"],
        how = "inner"
    )
    
    df_with_delay = segment_calcs.derive_stop_delay(
        df, 
        ("max_time_sec", "arrival_sec")
    )
    
    return df_with_delay
    
    



if __name__ == "__main__":
    logger.add("../logs/C6_calculate_stop_delay.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    scheduled_rt_stop_times = merge_scheduled_and_rt_stop_times(
        analysis_date
    )
    
    time1 = datetime.datetime.now()
    logger.info("merge scheduled and RT stop times and "
                f"calculate delay: {time1 - start}")
    
    stop_delay_by_segment = merge_in_stop_segments(
        scheduled_rt_stop_times, analysis_date)
    
    time2 = datetime.datetime.now()
    logger.info(f"merge in stop segments: {time2 - time1}")
    
    gdf = stop_delay_by_segment.compute()
    
    utils.geoparquet_gcs_export(
        gdf,
        SEGMENT_GCS,
        f"stop_segments_with_speed_delay_{analysis_date}"
    )    
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
