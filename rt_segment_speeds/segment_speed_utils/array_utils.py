"""
Functions for working with numpy arrays.
"""
import numpy as np
import pandas as pd

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