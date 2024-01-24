import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from scipy.spatial import cKDTree

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import gtfs_schedule_wrangling, wrangle_shapes     
from segment_speed_utils.project_vars import SEGMENT_GCS

def nearest_snap(line: shapely.LineString, point: shapely.Point) -> int:
    """
    Based off of this function,
    but we want to return the index value, rather than the point.
    https://github.com/UTEL-UIUC/gtfs_segments/blob/main/gtfs_segments/geom_utils.py
    """
    line = np.array(line.coords)
    point = np.array(point.coords)
    tree = cKDTree(line)
    
    # np_dist is array of distances of result
    # np_inds is array of indices of result
    np_dist, np_inds = tree.query(point, workers=-1, k=1)
    
    # We're looking for 1 nearest neighbor, so return 1st element in array
    return np_inds[0]


def add_nearest_vp_idx(
    vp_linestring: shapely.LineString, 
    stop: shapely.Point, 
    vp_idx_arr: np.ndarray
) -> int:
    """
    Index into where the nearest vp is to the stop,
    and return that vp_idx value from the vp_idx array.
    """
    idx = nearest_snap(vp_linestring, stop)
    
    return vp_idx_arr[idx]


def add_trio(
    nearest_value: int,
    vp_idx_arr: np.ndarray,
    timestamp_arr: np.ndarray,
    coords_arr: np.ndarray,
) -> tuple[np.ndarray]:
    """
    Try to grab at least 2 vp_idx, but optimally 3 vp_idx,
    including the nearest_vp_idx to 
    act as boundaries to interpolate against.
    3 points hopefully means 2 intervals where the stop could occur in
    to give us successful interpolation result.
    
    We don't have a guarantee that the nearest_vp_idx is before
    the stop, so let's just keep more boundary points.
    """
    start_idx = np.where(vp_idx_arr == nearest_value)[0][0]
    
    array_length = len(vp_idx_arr)

    # if we only have 2 values, we want to return all the possibilities
    if array_length <= 2:
        return vp_idx_arr, timestamp_arr, coords_arr
    
    # if it's the first value of the array 
    # and array is long enough for us to grab another value    
    elif (start_idx == 0) and (array_length > 2):
        start_pos = start_idx
        end_pos = start_idx + 3
        return (vp_idx_arr[start_pos: end_pos], 
                timestamp_arr[start_pos: end_pos], 
                coords_arr[start_pos: end_pos])
    
    # if it's the last value in the array, still grab 3 vp_idx
    elif start_idx == array_length - 1:
        start_pos = start_idx-2
        return (vp_idx_arr[start_pos: ], 
                timestamp_arr[start_pos: ],
                coords_arr[start_pos: ]
               )
    
    # (start_idx > 0) and (array_length > 2):
    else: 
        start_pos = start_idx - 1
        end_pos = start_idx + 2
        return (vp_idx_arr[start_pos: end_pos], 
                timestamp_arr[start_pos: end_pos],
                coords_arr[start_pos: end_pos]
               )


def merge_stop_vp_for_nearest_neighbor(
    stop_times: gpd.GeoDataFrame,
    vp_condensed: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    
    gdf = pd.merge(
        stop_times.rename(
            columns = {"geometry": "stop_geometry"}).set_geometry("stop_geometry"),
        vp_condensed.rename(
            columns = {"vp_primary_direction": "stop_primary_direction"}),
        on = ["trip_instance_key", "stop_primary_direction"],
        how = "inner"
    )
    
    return gdf