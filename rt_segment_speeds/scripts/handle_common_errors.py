"""
Tag rows where we know errors are occurring when 
we find the nearest vp.

Error 1: nearest_vp_idx = subseq_vp_idx.
We cannot interpolate if the 2 endpoints are the same points.

Error 2: change from the prior vp_idx value is negative. 
We expect monotonically increasing vp_idx across stops, 
which then translate to arrival times that are increasing.

For loop/inlining shapes, esp near origin 
(which is also destination), we can select a vp that's happening
at the end of the trip and attach to the origin stop.
This results in an arrival time for stop 1 that occurs after arrival time
for stop 2.
This happens because vp_primary_direction might be Unknown

"""
import datetime
import pandas as pd

from segment_speed_utils import array_utils, helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, PROJECT_CRS, CONFIG_PATH
from shared_utils import rt_dates
    

if __name__ == "__main__":
    
    analysis_date = rt_dates.DATES["sep2023"]
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")

    NEAREST_VP = f"{STOP_SEG_DICT['stage2']}_{analysis_date}"

    start = datetime.datetime.now()
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}projection/{NEAREST_VP}.parquet"
    )    
    
    WINDOW = 3
    df = array_utils.rolling_window_make_array(
        df, 
        window = WINDOW, rolling_col = "nearest_vp_idx"
    )
    df = array_utils.rolling_window_make_array(
        df, 
        window = WINDOW, rolling_col = "stop_meters"
    )
    
    df.to_parquet(
        f"{SEGMENT_GCS}projection/nearest_vp_error_{analysis_date}.parquet")

    end = datetime.datetime.now()
    print(f"execution time: {end - start}")