"""
Quick aggregation for speedmap segment speed averages.
"""
import datetime
import pandas as pd
import sys

from loguru import logger

from update_vars import GTFS_DATA_DICT, SEGMENT_GCS
from average_segment_speeds import segment_averages, OPERATOR_COLS

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    segment_type = "speedmap_segments"
    
    dict_inputs = GTFS_DATA_DICT[segment_type]
    
    SHAPE_STOP_COLS = [*dict_inputs["shape_stop_cols"]]
    ROUTE_DIR_COLS = [*dict_inputs["route_dir_cols"]]
    STOP_PAIR_COLS = [*dict_inputs["stop_pair_cols"]]
    
    SHAPE_SEG_FILE = dict_inputs["shape_stop_single_segment"]
    ROUTE_SEG_FILE = dict_inputs["route_dir_single_segment"]
    
    for analysis_date in analysis_date_list:
                
        segment_averages(
            [analysis_date], 
            segment_type, 
            group_cols = OPERATOR_COLS + SHAPE_STOP_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS,
            export_file = SHAPE_SEG_FILE
        )
        
        segment_averages(
            [analysis_date], 
            segment_type, 
            group_cols = OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS,
            export_file = ROUTE_SEG_FILE
        )
    
    '''
    from segment_speed_utils.project_vars import weeks_available
    
    ROUTE_SEG_FILE = dict_inputs["route_dir_multi_segment"]
    
    for one_week in weeks_available:
        
         segment_averages(
            one_week, 
            segment_type, 
            group_cols = OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS + ["weekday_weekend"],
            export_file = ROUTE_SEG_FILE
        )    
    '''