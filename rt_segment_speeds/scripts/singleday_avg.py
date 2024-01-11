"""
Get a single day's avg speeds by stop segments
aggregated to route-direction.
"""
import datetime
import pandas as pd

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH
from avg_speeds_by_segment import calculate_avg_speeds

import multiday_avg

if __name__ == "__main__":

    from segment_speed_utils.project_vars import analysis_date_list
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    MAX_SPEED = STOP_SEG_DICT["max_speed"]
    SEGMENT_SPEEDS_BY_ROUTE_DIR = STOP_SEG_DICT["stage8"]
    
    for analysis_date in analysis_date_list:
        df = multiday_avg.concatenate_speeds([analysis_date])

        operator_cols = [
            "schedule_gtfs_dataset_key", 
            "base64_url", "organization_source_record_id", "organization_name"
        ]
        shape_stop_cols = ["shape_id", "stop_sequence", "stop_id"]
        route_dir_cols = ["route_id", "direction_id", "stop_id", "stop_pair"]
        time_cols = ["peak_offpeak", "weekday_weekend"]

        daytype_peak_route_dir_speeds = calculate_avg_speeds(
            df[df.speed_mph <= MAX_SPEED],
            operator_cols + route_dir_cols + time_cols
        )

        mean_stop_seq = multiday_avg.rough_stop_sequence_ordering(
            df[df.speed_mph <= MAX_SPEED],
            operator_cols + route_dir_cols + time_cols        
        )

        daytype_peak_route_dir_speeds = pd.merge(
            daytype_peak_route_dir_speeds, 
            mean_stop_seq,
            on = operator_cols + route_dir_cols + time_cols,
            how = "inner"
        )    

        daytype_peak_route_dir_speeds.to_parquet(
            f"{SEGMENT_GCS}{SEGMENT_SPEEDS_BY_ROUTE_DIR}_{analysis_date}.parquet"
        )  
        
        del df, daytype_peak_route_dir_speeds, mean_stop_seq
    
    end = datetime.datetime.now()
    print(f"single up speeds for: {analysis_date_list}: {end - start}")