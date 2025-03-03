"""
Nearest neighbor utility functions.
"""
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from segment_speed_utils import helpers     
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import geo_utils

def merge_stop_vp_for_nearest_neighbor(
    stop_times: gpd.GeoDataFrame,
    analysis_date: str,
    **kwargs
) -> gpd.GeoDataFrame:
    """
    Merge stop times file with vp.
    vp gdf has been condensed so that all the vp coords
    make up coordinates of a linestring.
    """
    VP_NN = GTFS_DATA_DICT.speeds_tables.vp_condensed_line
    
    vp_condensed = gpd.read_parquet(
        f"{SEGMENT_GCS}{VP_NN}_{analysis_date}.parquet",
        columns = ["trip_instance_key", 
                   "vp_idx", "vp_primary_direction", 
                   "geometry"],
        **kwargs
    )

    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = True,
        filters = [[("shape_array_key", "in", stop_times.shape_array_key.tolist())]]
    ).rename(
        columns = {"geometry": "shape_geometry"}
    ).dropna(subset="shape_geometry")
    
    gdf = pd.merge(
        stop_times.rename(
            columns = {"geometry": "stop_geometry"}
        ).set_geometry("stop_geometry").to_crs(PROJECT_CRS),
        vp_condensed.to_crs(PROJECT_CRS).rename(
            columns = {"geometry": "vp_geometry"}),
        on = "trip_instance_key",
        how = "inner"
    ).merge(
        shapes,
        on = "shape_array_key",
        how = "inner"
    )

    # Calculate stop_meters, which is the stop geometry
    # projected onto shape_geometry and is interpreted as
    # stop X is Y meters along shape
    if "stop_meters" not in gdf.columns:
        gdf = gdf.assign(
            stop_meters = gdf.shape_geometry.project(gdf.stop_geometry),
        )

    return gdf


def find_nearest_points(
    vp_coords_array: np.ndarray, 
    target_stop: shapely.Point, 
    vp_idx_array: np.ndarray,
) -> np.ndarray:
    """    
    vp_coords_line is all the vehicle positions strung together as 
    coordinates in a linestring.
    The target point is a stop.

    We want to find the k nearest points before/after a stop.
    Start with k=5.
    Returns an array that gives the indices that are the nearest k points 
    (ex: nearest 5 vp to each stop).
    """
    indices = geo_utils.nearest_snap(
        vp_coords_array, 
        target_stop, 
        k_neighbors = 5
    )
        
    # nearest neighbor returns self.N 
    # if there are no nearest neighbor results found
    # if we want 10 nearest neighbors and 8th, 9th, 10th are all
    # the same result, the 8th will have a result, then 9th and 10th will
    # return the length of the array (which is out-of-bounds)
    # using vp_coords_array keeps too many points (is this because coords can be dupes?)
    indices2 = indices[indices < vp_idx_array.size]
    
    return indices2


def filter_to_nearest2_vp(
    nearest_vp_coords_array: np.ndarray,
    shape_geometry: shapely.LineString,
    nearest_vp_idx_array: np.ndarray,
    stop_meters: float,
) -> tuple[np.ndarray]:
    """
    Take the indices that are the nearest.
    Filter the vp coords down and project those against the shape_geometry.
    Calculate how close those nearest k vp are to a stop (as they travel along a shape).
    
    Filter down to the nearest 2 vp before and after a stop.
    If there isn't one before or after, a value of -1 is returned.
    """
    # Project these vp coords to shape geometry and see how far it is
    # from the stop's position on the shape
    nearest_vp_projected = np.asarray(
        [shape_geometry.project(shapely.Point(i)) 
         for i in nearest_vp_coords_array]
    )

    # Negative values are before the stop
    # Positive values are vp after the stop
    before_indices = np.where(nearest_vp_projected - stop_meters < 0)[0]
    after_indices = np.where(nearest_vp_projected - stop_meters > 0)[0]
    
    # Set missing values when we're not able to find a nearest neighbor result
    # use -1 as vp_idx (since this is not present in vp_usable)
    # and zeroes for meters
    before_idx = -1
    after_idx = -1
    before_vp_meters = 0
    after_vp_meters = 0
    
    # Grab the closest vp before a stop (-1 means array was empty)
    if before_indices.size > 0:
        before_idx = nearest_vp_idx_array[before_indices][-1] 
        before_vp_meters = nearest_vp_projected[before_indices][-1]
   
    # Grab the closest vp after a stop (-1 means array was empty)
    if after_indices.size > 0:
        after_idx = nearest_vp_idx_array[after_indices][0]
        after_vp_meters = nearest_vp_projected[after_indices][0]
    
    return before_idx, after_idx, before_vp_meters, after_vp_meters

    
def two_nearest_neighbor_near_stop(
    vp_direction_array: np.ndarray,
    vp_geometry: shapely.LineString,
    vp_idx_array: np.ndarray,
    stop_geometry: shapely.Point,
    opposite_stop_direction: str,
    shape_geometry: shapely.LineString,
    stop_meters: float
) -> np.ndarray: 
    """
    Each row stores several arrays related to vp.
    vp_direction is an array, vp_idx is an array,
    and the linestring of vp coords can be coerced into an array.
    
    When we're doing nearest neighbor search, we want to 
    first filter the full array down to valid vp
    before snapping it.
    """        
    # These are the valid index values where opposite direction 
    # is excluded       
    valid_indices = (vp_direction_array != opposite_stop_direction).nonzero()   
    
    # These are vp coords where index values of opposite direction is excluded
    valid_vp_coords_array = np.array(vp_geometry.coords)[valid_indices]
    
    # These are the subset of vp_idx values where opposite direction is excluded
    valid_vp_idx_array = np.asarray(vp_idx_array)[valid_indices]  
    
    nearest_indices = find_nearest_points(
        valid_vp_coords_array, 
        stop_geometry, 
        valid_vp_idx_array,
    )
 
    before_vp, after_vp, before_meters, after_meters = filter_to_nearest2_vp(
        valid_vp_coords_array[nearest_indices], # subset of coords in nn
        shape_geometry,
        valid_vp_idx_array[nearest_indices], # subset of vp_idx in nn
        stop_meters,
    )
    
    return before_vp, after_vp, before_meters, after_meters
