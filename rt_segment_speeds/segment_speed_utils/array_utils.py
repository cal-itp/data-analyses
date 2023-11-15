"""
Functions for working with numpy arrays.
"""
import numpy as np
import pandas as pd

from numba import jit

def get_index(array: np.ndarray, item) -> int:
    """
    Find the index for a certain value in an array.
    """
    #https://www.geeksforgeeks.org/how-to-find-the-index-of-value-in-numpy-array/
    for idx, val in np.ndenumerate(array):
        if val == item:
            return idx[0]

        
def subset_array_by_indices(
    array: np.ndarray, 
    start_end_tuple: tuple
):
    """
    Subset an array using index positions.
    
    https://stackoverflow.com/questions/31789187/find-indices-of-large-array-if-it-contains-values-in-smaller-array
    https://stackoverflow.com/questions/5508352/indexing-numpy-array-with-another-numpy-array  
    """
    lower_idx = start_end_tuple[0]
    upper_idx = start_end_tuple[-1] + 1
    
    return array[lower_idx: upper_idx]
        
    
def include_prior(
    array: np.ndarray, value: int
) -> np.ndarray:
    """
    For a given stop sequence value, find the prior and 
    subsequent stop sequence and return an array.
    """
    idx = get_index(array, value)
    upper_bound = idx + 1 
    
    if len(array) > upper_bound:
        subset_array = array[idx-1: idx+1]
    
        # For the first stop sequence, there is no prior, 
        # but the result of that is grabbing nothing
        if len(subset_array) == 0:
            subset_array  = array[idx: upper_bound]
        
    else:
        subset_array = array[idx-1:]
    
    return subset_array


def cut_shape_by_origin_destination(
    shape_distances_array: np.ndarray,
    origin_destination: tuple
) -> np.ndarray:  
    """
    Input either a shape_projected or cumulative_distances array, 
    along with an origin/destination (numeric). 
    
    Returns the indices of shape_distances_array that are between
    origin and destination. 
    
    Note: handling loops or inlining shapes, we don't know if the origin
    or destination is greater. It's possible that origin is further out from
    the start of the shape, and the destination is closer, so it looks like 
    we're interested in going from 500m out to 200m out.
    This is ok - handle this with np.min/np.max and grab indices between 
    an upper and lower bound.
    """
    origin, destination = origin_destination[0], origin_destination[1]
    
    # https://stackoverflow.com/questions/16343752/numpy-where-function-multiple-conditions
    # https://stackoverflow.com/questions/66755507/how-to-index-an-array-with-its-indices-in-numpy
     
    shape_dist_subset_indices = np.argwhere(
        (shape_distances_array >= np.min([origin, destination])) & 
        (shape_distances_array <= np.max([origin, destination]))
    ).flatten() 
    # flatten so it's 1d array

    return shape_dist_subset_indices


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