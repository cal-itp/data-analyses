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


def get_direction_vector(
    start: shapely.geometry.Point, 
    end: shapely.geometry.Point
) -> tuple:
    """
    Given 2 points (in a projected CRS...not WGS84), return a 
    tuple that shows (delta_x, delta_y).

    https://www.varsitytutors.com/precalculus-help/find-a-direction-vector-when-given-two-points
    https://stackoverflow.com/questions/17332759/finding-vectors-with-2-points

    """
    return ((end.x - start.x), (end.y - start.y))
    
    
def distill_array_into_direction_vector(array: np.ndarray) -> tuple:
    """
    Given an array of n items, let's take the start/end of that.
    From start/end, we can turn 2 coordinate points into 1 distance vector.
    Distance vector is a tuple that equals (delta_x, delta_y).
    """
    origin = array[0]
    destination = array[-1]
    return get_direction_vector(origin, destination)


def get_vector_norm(vector: tuple) -> float:
    """
    Get the length (off of Pythagorean Theorem) by summing up
    the squares of the components and then taking square root.
    
    Use Pythagorean Theorem to get unit vector. Divide the vector 
    by the length of the vector to get unit/normalized vector.
    This equation tells us what we need to divide by.
    """
    return np.sqrt(vector[0]**2 + vector[1]**2)


def get_normalized_vector(vector: tuple) -> tuple:
    """
    Apply Pythagorean Theorem and normalize the vector of distances.
    """
    x_norm = vector[0] / get_vector_norm(vector)
    y_norm = vector[1] / get_vector_norm(vector)

    return (x_norm, y_norm)


def dot_product(vec1: tuple, vec2: tuple) -> float:
    """
    Take the dot product. Multiply the x components, the y components, and 
    sum it up.
    """
    return vec1[0]*vec2[0] + vec1[1]*vec2[1]



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



# Take current stop
# find stop_sequence before and after

## NEED THIS
#stop_geom_array = np.array(gdf2.stop_geometry)
#stop_seq_array = np.array(gdf2.stop_sequence)

def super_project(
    current_stop_seq: int,
    shape_geometry: shapely.geometry.LineString,
    stop_geometry_array: np.ndarray,
    stop_sequence_array: np.ndarray,
):
    
    # Set up: project each of the coordinates in the shape_geometry
    # to be shape_meters
    shape_dist_array = project_list_of_coords(
        shape_geometry, [], use_shapely_coords = True)
    
    shape_dist_ordered = np.unique(shape_dist_array)
    
    # (1) Given a stop sequence value, find the stop_sequence values 
    # just flanking it (prior and subsequent).
    # this is important especially because stop_sequence does not have 
    # to be increasing in increments of 1, but it has to be monotonically increasing
    subset_seq = include_prior_and_subsequent(
        stop_sequence_array, current_stop_seq)
    print(f"subset of stop sequences: {subset_seq}")
    
    # (2) Grab relevant subset based on stop sequence values to get stop geometry subset
    # https://stackoverflow.com/questions/5508352/indexing-numpy-array-with-another-numpy-array
    subset_stop_geom = stop_geometry_array[subset_seq]
    print(f"subset of stop geometry: {subset_stop_geom}")
    
    # (3) Project this vector of 3 stops
    # because we need to know which part to subset
    # off of the shape's shape_meters array
    subset_stop_proj = project_list_of_coords(
        shape_geometry, subset_stop_geom)
    
    print(f"subset stops projected: {subset_stop_proj}")
    
    # (4) Get the subset of shape_path points that
    # spans a start distance position and end distance position 
    # We have 2 stops, and let's grab the chunk of the shape that spans that
    # https://stackoverflow.com/questions/16343752/numpy-where-function-multiple-conditions

    # the start_dist and end_dist take the prior/subsequent stop
    # to use to check against direction
    start_stop = subset_stop_proj[0]
    end_stop = subset_stop_proj[-1]
    
    print(f"origin: {start_stop}, destination: {end_stop}")
    
    # typical, the end_stop should be progressing further
    if end_stop > start_stop: 
        shape_dist_subset = cut_shape_projected_by_origin_destination(
            shape_dist_array, 
            (start_stop, end_stop)
        )
        print(f"cut shape by od: {shape_dist_subset}")
    
    elif end_stop < start_stop:
        shape_dist_subset = cut_shape_projected_by_origin_destination(
            shape_dist_ordered, 
            (end_stop, start_stop)
        )
        print(f"cut shape by od: {shape_dist_subset}")        
    
        shape_dist_subset = np.flip(shape_dist_subset)
        print(f"flipped: cut shape by od: {shape_dist_subset}")        

    
    # Interpolate again so we change the shape_meters back into coordinate points
    subset_shape_geom = interpolate_projected_points(
        shape_geometry, shape_dist_subset)
    
    print(f"interpolated subset of shape: {subset_shape_geom}")
    
    # If the stop vector and shape vector run in the same direction,
    # then we want to cut a segment from prior to current stop.
    dot_prod = find_if_two_arrays_same_direction(
        subset_stop_geom,
        subset_shape_geom
    )
    
    return dot_prod, subset_stop_geom, subset_shape_geom


def find_if_two_arrays_same_direction(
    subset_stop_geom_array: np.ndarray, 
    subset_shape_geom_array: np.ndarray
) -> float:
    """
    https://stackoverflow.com/questions/21030391/how-to-normalize-a-numpy-array-to-a-unit-vector
    """
    # Get vectors for stop and shape
    stop_vec = distill_array_into_direction_vector(
        subset_stop_geom_array)
    shape_vec = distill_array_into_direction_vector(
        subset_shape_geom_array)
    
    print(f"stop vector: {stop_vec}")
    print(f"shape vector: {shape_vec}")
    
    # Normalize the vectors (divide by length) to get unit vector
    stop_norm = get_normalized_vector(stop_vec)
    shape_norm = get_normalized_vector(shape_vec)
    
    print(f"stop_norm: {stop_norm}, shape_norm: {shape_norm}")
    
    dot_result = dot_product(stop_norm, shape_norm)
    
    print(f"dot product: {dot_result}")
    
    return dot_result