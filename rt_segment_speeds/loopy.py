import numpy as np
import pandas as pd
import shapely


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
        
        
def include_prior_and_subsequent(
    array: np.ndarray, value: int
) -> np.ndarray:
    """
    For a given stop sequence value, find the prior and 
    subsequent stop sequence and return an array.
    """
    idx = get_index(array, value)
    
    subset_array = array[idx-1: idx+2]
    
    # For the first stop sequence, there is no prior, 
    # but the result of that is grabbing nothing
    if len(subset_array) == 0:
        subset_array  = array[idx: idx+2]
        
    # for the last stop sequence, there is no subsequent,
    # and the subset of array grabs 2 items, which is ok        
    
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


def cut_shape_projected_by_origin_destination(
    shape_projected_array: np.ndarray,
    origin_destination: tuple
) -> np.ndarray:  
    origin, destination = origin_destination[0], origin_destination[1]
    
    return shape_projected_array[
        (np.where(
            (shape_projected_array >= origin) & 
            (shape_projected_array <= destination))
        )]


def get_direction_vector(
    start: shapely.geometry.Point, 
    end: shapely.geometry.Point
) -> tuple:
    """
    Given 2 points (in a projected CRS...not WGS84), return a 
    tuple that shows (delta_x, delta_y).

    https://www.varsitytutors.com/precalculus-help/find-a-direction-vector-when-given-two-points
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