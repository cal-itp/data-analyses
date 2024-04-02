"""
Concatenate 
"""
import pandas as pd
from segment_speed_utils import time_helpers
from segment_speed_utils.project_vars import SCHED_GCS

if __name__ == "__main__":
    
        from update_vars import CONFIG_DICT
        MONTHLY_SERVICE = CONFIG_DICT["monthly_scheduled_service_file"]

        year_list = [2023, 2024]
        
        df = pd.concat(
            [pd.read_parquet(
                f"{SCHED_GCS}scheduled_service_by_route_{y}.parquet") 
             for y in year_list], 
            axis=0, ignore_index=True
        )
        
        df = df.assign(
            day_name = df.day_type.map(time_helpers.DAY_TYPE_DICT)
        )
        
        df.to_parquet(
            f"{SCHED_GCS}{MONTHLY_SERVICE}.parquet"
        ) 