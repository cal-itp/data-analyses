"""
Quick aggregation for speedmap segment speed averages.
"""
import datetime
import pandas as pd
import sys

from loguru import logger

from average_segment_speeds import single_day_segment_averages, multi_day_segment_averages

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    segment_type = "speedmap_segments"
    
    for analysis_date in analysis_date_list:
                
        single_day_segment_averages(analysis_date, segment_type)
    
    '''
    from segment_speed_utils.project_vars import weeks_available
    for one_week in weeks_available:
        
        multi_day_segment_averages(one_week, segment_type)
    '''