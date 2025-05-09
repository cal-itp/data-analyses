"""
Use projected road end points, calculate speeds
on the segment between each road segment.
"""
import datetime
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from modeled_rt_stop_times_speeds import speed_arrays_by_segment_trip

if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_road_speeds.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    from segment_speed_utils.project_vars import test_dates
    
    for analysis_date in test_dates:
    
        start = datetime.datetime.now()
    
        segment_type = "modeled_road_segments"
        
        INPUT_FILE = GTFS_DATA_DICT.modeled_vp.resampled_vp
        ROAD_ENDPOINTS_PROJECTED = GTFS_DATA_DICT[segment_type].road_endpoints_projected
        EXPORT_FILE = GTFS_DATA_DICT[segment_type].speeds_wide
        trip_stop_cols = [*GTFS_DATA_DICT[segment_type].trip_stop_cols]
        
        df = pd.read_parquet(
            f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet"
        )
             
        road_cutoffs = pd.read_parquet(
            f"{SEGMENT_GCS}{ROAD_ENDPOINTS_PROJECTED}_{analysis_date}.parquet",
        )
            
        gdf = pd.merge(
            df,
            road_cutoffs,
            on = "trip_instance_key",
            how = "inner"
        )
        
        results = speed_arrays_by_segment_trip(
            gdf,
            trip_stop_cols,
            cutoffs_col = "road_projected_dist",
        )
        
        results.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
        )
    
        end = datetime.datetime.now()
        logger.info(f"{segment_type}  {analysis_date}: speeds: {end - start}")
