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
    
    idx = nearest_snap(vp_linestring, stop)
    
    return vp_idx_arr[idx]


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