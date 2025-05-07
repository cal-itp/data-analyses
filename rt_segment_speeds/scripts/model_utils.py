"""
Move to vp_transform?
"""
import geopandas as gpd
import numpy as np
import pandas as pd

from numba import jit

from segment_speed_utils.project_vars import PROJECT_CRS
from shared_utils.rt_utils import MPH_PER_MPS


def project_point_onto_shape(
    gdf: gpd.GeoDataFrame, 
    line_geom: str, 
    point_geom: str,
    crs: str = PROJECT_CRS
) -> gpd.GeoDataFrame:
    """
    Project point on line.
    Double check -- this function probably lives somewhere already. 
    """
    gdf = gdf.assign(
        projected_meters = gdf[line_geom].to_crs(crs).project(
            gdf[point_geom].to_crs(crs)
        )
    )
    
    return gdf


@jit(nopython=True)
def resample_timestamps(
    timestamp_arr: np.ndarray, 
    seconds_step: int
) -> np.ndarray:
    """
    resample timestamp array by every X seconds
    """
    return np.arange(min(timestamp_arr), max(timestamp_arr), step=seconds_step)


@jit(nopython=True)
def interpolate_distances_for_resampled_timestamps(
    resampled_timestamp_arr: np.ndarray, 
    original_timestamp_arr: np.ndarray, 
    projected_meters_arr: np.ndarray
) -> np.ndarray:
    """
    Interpolate and get (modeled/interpolated) Y's against X's.
    We have original X array, resampled X with more observations,
    and now we want to fill in those Y's in between with 
    numpy linear interpolation.
    """
    return np.interp(
        resampled_timestamp_arr, 
        original_timestamp_arr,
        projected_meters_arr
    )

def check_monotonically_increasing_condition(
    my_array: list
) -> np.ndarray:
    """
    Somehow store whether projecting stop position onto vp path is increasing or not.
    results that are True, we can calculate speeds for, otherwise, we shouldn't.
    
    Projecting stop positions onto actual vp path rather than shape (scheduled path)
    reduces the fluctuations and broadly solves the loop/inlining issue.
    """
    my_array2 = np.array(my_array)
    boolean_results = np.diff(my_array2) > 0
    
    return np.array(boolean_results) 


def calculate_meters_seconds_speed_deltas(
    one_trip_distance_arr: np.ndarray, 
    one_trip_timestamp_arr: np.ndarray,
    intervaled_distance_cutoffs: np.ndarray,
) -> tuple[np.ndarray]:
    """
    
    """
    delta_distances_series = []
    delta_seconds_series = []
    speed_series = []

    for i in range(0, len(intervaled_distance_cutoffs) - 1):
        # Find the cutoffs that define the segment
        cut1 = intervaled_distance_cutoffs[i]
        cut2 = intervaled_distance_cutoffs[i+1]
        
        # Grab indices for the relevant segment
        subset_indices = np.where((one_trip_distance_arr >= cut1) & (one_trip_distance_arr <= cut2))

        # Subset to the relevant portions of the array
        subset_distances = one_trip_distance_arr[subset_indices]
        subset_times = one_trip_timestamp_arr[subset_indices]
        
        # Return results if the subset of indices is not empty
        if len(subset_distances > 0):
            delta_distances = subset_distances.max() - subset_distances.min()
            delta_seconds = subset_times.max() - subset_times.min()
            one_speed = delta_distances / delta_seconds * MPH_PER_MPS
            
            delta_distances_series.append(delta_distances)
            delta_seconds_series.append(delta_seconds)
            speed_series.append(one_speed)
        
        # If we can't find the subset of indices, skip these results and save out NaNs
        # Need this otherwise rows won't match when we explode results later
        else:
            delta_distances_series.append(np.nan)
            delta_seconds_series.append(np.nan)
            speed_series.append(np.nan)
    
    return delta_distances_series, delta_seconds_series, speed_series