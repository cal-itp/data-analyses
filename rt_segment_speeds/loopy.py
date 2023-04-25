import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from typing import Literal

def interpolate_projected_points(
    shape_geometry: shapely.geometry.LineString,
    projected_list: list
):
    return [shape_geometry.interpolate(i) for i in projected_list]


def get_index(array: np.ndarray, item) -> int:
    """
    Find the index for a certain value in an array.
    """
    #https://www.geeksforgeeks.org/how-to-find-the-index-of-value-in-numpy-array/
    for idx, val in np.ndenumerate(array):
        if val == item:
            return idx[0]
        
        
def include_prior(
    array: np.ndarray, value: int
) -> np.ndarray:
    """
    For a given stop sequence value, find the prior and 
    subsequent stop sequence and return an array.
    """
    idx = get_index(array, value)
    
    subset_array = array[idx-1: idx+1]
    
    # For the first stop sequence, there is no prior, 
    # but the result of that is grabbing nothing
    if len(subset_array) == 0:
        subset_array  = array[idx: idx+1]
            
    return subset_array


def project_list_of_coords(
    shape_geometry: shapely.geometry.LineString,
    point_geom_list: list,
    use_shapely_coords: bool = False
) -> np.ndarray:
    if use_shapely_coords:
        # https://stackoverflow.com/questions/49330030/remove-a-duplicate-point-from-polygon-in-shapely
        # use simplify(0) to remove any points that might be duplicates
        return np.array(
            [shape_geometry.project(shapely.geometry.Point(p))
            for p in shape_geometry.simplify(0).coords])
    else:
        return np.array(
            [shape_geometry.project(i) for i in point_geom_list])


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


def array_to_geoseries(
    array: np.ndarray,
    geom_type: Literal["point", "line", "polygon"],
    crs: str = "EPSG:3310"
)-> gpd.GeoSeries: 
    """
    Turn array back into geoseries.
    """
    if geom_type == "point":
        gdf = gpd.GeoSeries(array, crs=crs)
        
    elif geom_type == "line":
        gdf = gpd.GeoSeries(
            shapely.geometry.LineString(array), 
            crs=crs)
        
    elif geom_type == "polygon":
        gdf = gpd.GeoSeries(
            shapely.geometry.Polygon(array),
            crs = crs)
    
    return gdf