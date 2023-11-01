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
import numpy as np
import pandas as pd

from numba import jit

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, PROJECT_CRS, CONFIG_PATH
from shared_utils import rt_dates

def rolling_window_make_array(
    df: pd.DataFrame, 
    window: int, 
    rolling_col: str
) -> pd.DataFrame:
    # https://stackoverflow.com/questions/47482009/pandas-rolling-window-to-return-an-array
    df[f"rolling_{rolling_col}"] = [
        np.asarray(window)for window in 
        df.groupby("trip_instance_key")[rolling_col].rolling(
            window = window, center=True)
    ]
    
    is_monotonic_series = np.vectorize(monotonic_check)(df[f"rolling_{rolling_col}"])
    df[f"{rolling_col}_monotonic"] = is_monotonic_series
    
    return df
    
@jit(nopython=True)
def monotonic_check(arr: np.ndarray) -> bool:
    """
    For an array, check if it's monotonically increasing. 
    https://stackoverflow.com/questions/4983258/check-list-monotonicity
    """
    diff_arr = np.diff(arr)
    
    if np.all(diff_arr >= 0):
        return True
    else:
        return False
    

if __name__ == "__main__":
    
    analysis_date = rt_dates.DATES["sep2023"]
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")

    NEAREST_VP = f"{STOP_SEG_DICT['stage2']}_{analysis_date}"

    start = datetime.datetime.now()
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}projection/{NEAREST_VP}.parquet"
    )    
    
    window = 3
    df = rolling_window_make_array(df, window = window, rolling_col = "nearest_vp_idx")
    df = rolling_window_make_array(df, window = window, rolling_col = "stop_meters")
    
    df.to_parquet(
        f"{SEGMENT_GCS}projection/nearest_vp_error_{analysis_date}.parquet")

    end = datetime.datetime.now()
    print(f"execution time: {end - start}")