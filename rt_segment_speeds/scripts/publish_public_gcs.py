"""
Grab all files in the rollup
"""
import datetime
import pandas as pd

from segment_speed_utils import time_series_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, PUBLIC_GCS


if __name__ == "__main__":

    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    DATASETS = [
        STOP_SEG_DICT["route_dir_single_segment"]
    ]
    
    for d in DATASETS:
        
        start = datetime.datetime.now()

        df = time_series_utils.concatenate_datasets_across_months(d)
        df.to_parquet(f"{PUBLIC_GCS}speeds/{d}.parquet")
                
        end = datetime.datetime.now()
        print(f"save {d} to public GCS: {end - start}")
