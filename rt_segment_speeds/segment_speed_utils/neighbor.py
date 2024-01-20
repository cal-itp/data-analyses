import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from scipy.spatial import cKDTree

from segment_speed_utils import gtfs_schedule_wrangling     
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
    gdf: gpd.GeoDataFrame,
    vp_linestring_col: str = "geometry",
    stop_point_col: str = "start",
    vp_idx_array_col: str = "vp_idx",
) -> gpd.GeoDataFrame:
    """
    """
    results = []
    
    for row in gdf.itertuples():
        
        vp_linestring = getattr(row, vp_linestring_col)
        stop = getattr(row, stop_point_col)
        vp_idx_array = getattr(row, vp_idx_array_col)
        
        idx = nearest_snap(vp_linestring, stop)
        
        results.append(vp_idx_array[idx])
        
    gdf = gdf.assign(
        nearest_vp_idx = results
    )
    
    return gdf


def merge_stops_with_vp(
    stop_times: gpd.GeoDataFrame, 
    vp_condensed: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    """
    gdf = pd.merge(
        stop_times.rename(columns = {"geometry": "start"}).set_geometry("start"),
        vp_condensed.rename(columns = {
            "vp_primary_direction": "stop_primary_direction"}),
        on = ["trip_instance_key", "stop_primary_direction"],
        how = "inner"
    )
    
    return gdf