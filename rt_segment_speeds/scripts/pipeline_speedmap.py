"""
Run nearest_vp_to_stop.py, 
interpolate_stop_arrivals.py,
and calculate_speed_from_stop_arrivals.py for speedmap_segments.
"""
import datetime
import pandas as pd
import sys

from loguru import logger
from pathlib import Path
from typing import Optional

from update_vars import SEGMENT_GCS, GTFS_DATA_DICT

from nearest_vp_to_stop import nearest_neighbor_for_stop
from vp_around_stops import filter_to_nearest_two_vp
from interpolate_stop_arrival import interpolate_stop_arrivals
from stop_arrivals_to_speed import calculate_speed_from_stop_arrivals

segment_type = "speedmap_segments"


def concatenate_speedmap_proxy_arrivals_with_remaining(
    analysis_date: str,
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    """
    Nearest vp and interpolation was done just for extra
    speedmap segments.
    
    Only 6% of segments had extra segments / cutpoints 
    (proxy stops), so we need not run the entire nearest neighbor
    redundantly. We can do nearest neighbor on just those 6% 
    and concatenate the full results from rt_stop_times pipeline, 
    which is every trip-stop anyway.
    
    Append those results and all the stop arrivals into 
    speed calculation.
    """
    PROXY_STOP_ARRIVALS_FILE = GTFS_DATA_DICT.speedmap_segments.stage3
    OTHER_STOP_ARRIVALS_FILE = GTFS_DATA_DICT.rt_stop_times.stage3
    SPEEDMAP_STOP_ARRIVALS = GTFS_DATA_DICT.speedmap_segments.stage3b
    trip_stop_cols = [*GTFS_DATA_DICT.speedmap_segments.trip_stop_cols]
    
    proxy_arrivals = pd.read_parquet(
        f"{SEGMENT_GCS}{PROXY_STOP_ARRIVALS_FILE}_{analysis_date}.parquet"
    )

    other_arrivals= pd.read_parquet(
        f"{SEGMENT_GCS}{OTHER_STOP_ARRIVALS_FILE}_{analysis_date}.parquet"
    )
        
    df = pd.concat([
        proxy_arrivals, 
        other_arrivals
    ], axis=0, ignore_index=True
    ).sort_values(
        trip_stop_cols
    ).reset_index(drop=True)
    
    df.to_parquet(
        f"{SEGMENT_GCS}{SPEEDMAP_STOP_ARRIVALS}_{analysis_date}.parquet"
    )
    
    return
    

if __name__ == "__main__":
        
    from segment_speed_utils.project_vars import analysis_date_list
    
    print(f"segment_type: {segment_type}")
    
    for analysis_date in analysis_date_list:      
                        
        LOG_FILE = "../logs/nearest_vp.log"
        logger.add(LOG_FILE, retention="3 months")
        logger.add(sys.stderr, 
                   format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
                   level="INFO")
    
        nearest_neighbor_for_stop(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        )    
                
        filter_to_nearest_two_vp(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        )
        
        logger.remove()
    
        LOG_FILE = "../logs/interpolate_stop_arrival.log"
        logger.add(LOG_FILE, retention="3 months")
        logger.add(sys.stderr, 
                   format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
                   level="INFO")

        interpolate_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )
        
        logger.remove()
        
        t0 = datetime.datetime.now()
        concatenate_speedmap_proxy_arrivals_with_remaining(
            analysis_date,
            config_path = GTFS_DATA_DICT
        )
        
        t1 = datetime.datetime.now()
        print(f"concatenate proxy arrivals with rt_stop_times: {t1 - t0}")
        
        LOG_FILE = "../logs/speeds_by_segment_trip.log"
        logger.add(LOG_FILE, retention="3 months")
        logger.add(sys.stderr, 
                   format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
                   level="INFO")

        calculate_speed_from_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        )
        
        logger.remove()
