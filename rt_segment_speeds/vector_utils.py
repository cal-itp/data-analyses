import numpy as np
import pandas as pd
import shapely

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