import folium
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from segment_speed_utils import wrangle_shapes

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


#-------------------------------------------------------#
#
#-------------------------------------------------------#
def get_shape_components(
    shape_geometry: shapely.geometry.LineString,
) -> tuple:
    """
    For a shape, we want to get the list of shapely.Points and
    a calculated cumulative distance array.
    """
    shape_coords_list = [shapely.Point(i) for 
                         i in shape_geometry.simplify(0).coords]
    
    # calculate the distance between current point and prior
    # need to remove the first point so that we can 
    # compare to the prior
    point_series_no_idx0 = wrangle_shapes.array_to_geoseries(
        shape_coords_list[1:],
        geom_type="point"
    )

    points_series = wrangle_shapes.array_to_geoseries(
        shape_coords_list, 
        geom_type="point"
    )
    
    distance_from_prior = np.array(
        point_series_no_idx0.distance(points_series)
    )
    
    # Based on distance_from_prior, now create a 
    # cumulative distance array, and append 0 to 
    # the beginning. We want length of this array to match the 
    # length of stop_sequence array
    cumulative_distances = np.array(
        [0] + list(np.cumsum(distance_from_prior))
    )
    
    return shape_coords_list, cumulative_distances


def adjust_stop_start_end_for_special_cases(
    shape_geometry: shapely.geometry.LineString,
    subset_stop_geometry: np.ndarray,
    subset_stop_projected: np.ndarray
) -> tuple:
    """
    """
    # (1a) Normal case: given a current stop and a prior stop
    # use this to get sense of direction, whether 
    # distance is increasing or decreasing as we move from prior to current stop
    if len(subset_stop_projected) > 1:
        start_stop = subset_stop_projected[0]
        end_stop = subset_stop_projected[-1]
    
        # Calculate distance between stops
        distance_between_stops = subset_stop_geometry[0].distance(
            subset_stop_geometry[-1])
    
    # (1b) Origin stop case: current stop only, cannot find prior stop
    # but we can set the start point to be the start of the shape
    else:
        start_stop = 0
        end_stop = subset_stop_projected[0]
        distance_between_stops = subset_stop_projected[0]
        
    # (2) We know distance between stops, so let's back out the correct
    # "end_stop". If the end_stop is actually going 
    # back closer to the start of the shape, we'll use subtraction.
    
    # Normal case
    if start_stop < end_stop:
        origin_stop = start_stop
        destin_stop = start_stop + distance_between_stops
        
        # change this to point
        origin_destination_geom = wrangle_shapes.interpolate_projected_points(
            shape_geometry, [origin_stop, destin_stop])
    
    # Case where inlining occurs, and now the bus is doubling back    
    elif start_stop > end_stop:
        origin_stop = start_stop
        destin_stop = start_stop - distance_between_stops
        
        # Flip this so when we order the subset of points, we 
        # correctly append the origin to the shape_coords_list 
        # (which is destination, since it has a lower value)
        origin_destination_geom = wrangle_shapes.interpolate_projected_points(
            shape_geometry, [destin_stop, origin_stop])
    
    # Case at origin, where there is no prior stop to look for
    elif start_stop == end_stop:
        origin_stop = 0
        destin_stop = start_stop
        
        origin_destination_geom = wrangle_shapes.interpolate_projected_points(
            shape_geometry, [origin_stop, destin_stop]
        )
    
    return origin_stop, destin_stop, origin_destination_geom


def super_project(
    current_stop_seq: int,
    shape_geometry: shapely.geometry.LineString,
    stop_geometry_array: np.ndarray,
    stop_sequence_array: np.ndarray,
):
    
    shape_coords_list, cumulative_distances = get_shape_components(
        shape_geometry)

    # (1) Given a stop sequence value, find the stop_sequence values 
    # just flanking it (prior and subsequent).
    # this is important especially because stop_sequence does not have 
    # to be increasing in increments of 1, but it has to be monotonically increasing
    subset_seq = include_prior(
        stop_sequence_array, current_stop_seq)
    
    #https://stackoverflow.com/questions/31789187/find-indices-of-large-array-if-it-contains-values-in-smaller-array
    idx_stop_seq = np.where(np.in1d(
        stop_sequence_array, subset_seq))[0]
    
    # (2) Grab relevant subset based on stop sequence values to get stop geometry subset
    # https://stackoverflow.com/questions/5508352/indexing-numpy-array-with-another-numpy-array    
    subset_stop_geom = subset_array_by_indices(
        stop_geometry_array,
        (idx_stop_seq[0], idx_stop_seq[-1])
    )
    
    # (3a) Project this vector of start/end stops
    subset_stop_proj = wrangle_shapes.project_list_of_coords(
        shape_geometry, subset_stop_geom)
    
    
    # (4) Handle various cases for first stop or last stop
    # and also direction of origin to destination stop
    # if destination stop actually is closer to the start of the shape,
    # that's ok, we'll use distance and make sure the direction is correct
    (origin_stop, destin_stop, 
     origin_destination_geom) = adjust_stop_start_end_for_special_cases(
        shape_geometry,
        subset_stop_geom, 
        subset_stop_proj
    )
        
    # (5) Find the subset from cumulative distances
    # that is in between our origin stop and destination stop
    idx_shape_dist = cut_shape_by_origin_destination(
        cumulative_distances,
        (origin_stop, destin_stop)
    )
            
    if len(cumulative_distances) < idx_shape_dist[-1]:
        subset_shape_geom = subset_array_by_indices(
            shape_coords_list, 
            (idx_shape_dist[0], idx_shape_dist[-2])
        )

    else:
         subset_shape_geom = subset_array_by_indices(
            shape_coords_list, 
            (idx_shape_dist[0], idx_shape_dist[-1])
        )
    
    # Attach the origin and destination, otherwise the segment
    # will not reach the actual stops, but will just grab the trunk portion
    subset_shape_geom_with_od = np.array(
        [origin_destination_geom[0]] + 
        subset_shape_geom + 
        [origin_destination_geom[-1]]
    )
    
    return subset_shape_geom_with_od, origin_destination_geom


def stop_segment_components_to_geoseries(
    subset_shape_geom_array: np.ndarray,
    subset_stop_geom_array: np.ndarray = [],
    crs: str = "EPSG:3310"
) -> tuple:#[gpd.GeoDataFrame]:
    """
    Turn segments and stops into geoseries so we can plot it easily.
    """
    stop_segment = wrangle_shapes.array_to_geoseries(
        subset_shape_geom_array, 
        geom_type="line",
        crs=crs
    )#.to_frame(name="stop_segment")
    
    if len(subset_stop_geom_array) > 0:
        related_stops = wrangle_shapes.array_to_geoseries(
            subset_stop_geom_array,
            geom_type="point",
            crs=crs
        )#.to_frame(name="surrounding_stops_geom")

        return stop_segment, related_stops
    else:
        return stop_segment
    
    
def plot_segments_and_stops(
    segment: gpd.GeoSeries, 
    stops: gpd.GeoSeries
):
    m = segment.explore(tiles="CartoDB Positron", name="segment")
    m = stops.explore(m=m, name="stops")

    folium.LayerControl().add_to(m)
    return m