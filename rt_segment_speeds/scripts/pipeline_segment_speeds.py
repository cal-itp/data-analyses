"""
Run nearest_vp_to_stop.py, 
interpolate_stop_arrivals.py,
and calculate_speed_from_stop_arrivals.py for stop_segments.
"""
import sys

from loguru import logger

from nearest_vp_to_stop import nearest_neighbor_for_stop
from vp_around_stops import filter_to_nearest_two_vp
from new_interpolate_stop_arrival import interpolate_stop_arrivals
from stop_arrivals_to_speed import calculate_speed_from_stop_arrivals
from update_vars import GTFS_DATA_DICT

segment_type = "stop_segments"

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    from shared_utils import rt_dates
    skip_me = rt_dates.DATES["jul2024"]
    
    print(f"segment_type: {segment_type}")
    
    analysis_date_list = [i for i in analysis_date_list if i != skip_me]
    
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
        
        logger.remove()
        
        filter_to_nearest_two_vp(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        )
        
        logger.remove()
        
        '''
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