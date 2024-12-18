"""
Nearest neighbor utility functions.
"""
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from segment_speed_utils import helpers, vp_transform     
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import geo_utils


def merge_stop_vp_for_nearest_neighbor(
    stop_times: gpd.GeoDataFrame,
    analysis_date: str,
    **kwargs
):
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
    ).rename(columns = {"geometry": "shape_geometry"})
    
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
    gdf = gdf.assign(
        stop_meters = gdf.shape_geometry.project(gdf.stop_geometry)
    )

    return gdf


def find_nearest_points(
    vp_coords_line: np.ndarray, 
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
        vp_coords_line, 
        target_stop, 
        k_neighbors = 5
    )
        
    # nearest neighbor returns self.N 
    # if there are no nearest neighbor results found
    # if we want 10 nearest neighbors and 8th, 9th, 10th are all
    # the same result, the 8th will have a result, then 9th and 10th will
    # return the length of the array (which is out-of-bounds)
    indices2 = indices[indices < vp_idx_array.size]
    
    return indices2


def filter_to_nearest2_vp(
    vp_coords_line: np.ndarray,
    shape_geometry: shapely.LineString,
    vp_idx_array: np.ndarray,
    stop_meters: float,
    indices_of_nearest: np.ndarray,
) -> tuple[np.ndarray]:
    """
    Take the indices that are the nearest.
    Filter the vp coords down and project those against the shape_geometry.
    Calculate how close those nearest k vp are to a stop (as they travel along a shape).
    
    Filter down to the nearest 2 vp before and after a stop.
    If there isn't one before or after, a value of -1 is returned.
    """
    # Subset the array of vp coords and vp_idx_array with 
    # the indices that show the nearest k neighbors.
    nearest_vp = vp_coords_line[indices_of_nearest]
    nearest_vp_idx = vp_idx_array[indices_of_nearest]
    
    # Project these vp coords to shape geometry and see how far it is
    # from the stop's position on the shape
    nearest_vp_projected = np.asarray(
        [shape_geometry.project(shapely.Point(i)) 
         for i in nearest_vp]
    )

    # Negative values are before the stop
    # Positive values are vp after the stop
    before_indices = np.where(nearest_vp_projected - stop_meters < 0)[0]
    after_indices = np.where(nearest_vp_projected - stop_meters > 0)[0]
    
    # Grab the closest vp before a stop (-1 means array was empty)
    if before_indices.size > 0:
        before_idx = nearest_vp_idx[before_indices][-1] 
        before_vp_meters = nearest_vp_projected[before_indices][-1]
    else:
        before_idx = -1
        before_vp_meters = 0
        
    # Grab the closest vp after a stop (-1 means array was empty)
    if after_indices.size > 0:
        after_idx = nearest_vp_idx[after_indices][0]
        after_vp_meters = nearest_vp_projected[after_indices][0]
    else:
        after_idx = -1
        after_vp_meters = 0
    
    return before_idx, after_idx, before_vp_meters, after_vp_meters


def subset_arrays_to_valid_directions(
    vp_direction_array: np.ndarray,
    vp_geometry: shapely.LineString,
    vp_idx_array: np.ndarray,
    stop_geometry: shapely.Point,
    stop_direction: str,
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
    opposite_direction = vp_transform.OPPOSITE_DIRECTIONS[stop_direction] 
    
    # These are the valid index values where opposite direction 
    # is excluded       
    valid_indices = (vp_direction_array != opposite_direction).nonzero()   

    # These are vp coords where index values of opposite direction is excluded
    valid_vp_coords_line = np.array(vp_geometry.coords)[valid_indices]
    
    # These are the subset of vp_idx values where opposite direction is excluded
    valid_vp_idx_arr = np.asarray(vp_idx_array)[valid_indices]  
    
    nearest_indices = find_nearest_points(
        valid_vp_coords_line, 
        stop_geometry, 
        valid_vp_idx_arr,
    )
 
    before_vp, after_vp, before_meters, after_meters = filter_to_nearest2_vp(
        valid_vp_coords_line,
        shape_geometry,
        valid_vp_idx_arr,
        stop_meters,
        nearest_indices,
    )
    
    return before_vp, after_vp, before_meters, after_meters