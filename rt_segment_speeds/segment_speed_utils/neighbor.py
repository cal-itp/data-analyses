"""
Nearest neighbor utility functions.
"""
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import gtfs_schedule_wrangling, vp_transform     
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT
from shared_utils import geo_utils


def add_nearest_vp_idx(
    vp_linestring: shapely.LineString, 
    stop: shapely.Point, 
    vp_idx_arr: np.ndarray
) -> int:
    """
    Index into where the nearest vp is to the stop,
    and return that vp_idx value from the vp_idx array.
    """
    idx = geo_utils.nearest_snap(vp_linestring, stop, k_neighbors=1)
    
    return vp_idx_arr[idx]

    
def merge_stop_vp_for_nearest_neighbor(
    stop_times: gpd.GeoDataFrame,
    analysis_date: str,
    **kwargs
) -> gpd.GeoDataFrame:
    VP_NN = GTFS_DATA_DICT.speeds_tables.vp_condensed_line
    
    vp_condensed = gpd.read_parquet(
        f"{SEGMENT_GCS}{VP_NN}_{analysis_date}.parquet",
        columns = ["trip_instance_key", 
                   "vp_idx", "vp_primary_direction", 
                   "geometry"],
        **kwargs
    ).to_crs(WGS84)

    gdf = pd.merge(
        stop_times.rename(
            columns = {"geometry": "stop_geometry"}
        ).set_geometry("stop_geometry").to_crs(WGS84),
        vp_condensed.rename(
            columns = {
                "geometry": "vp_geometry"
            }),
        on = "trip_instance_key",
        how = "inner"
    )
        
    return gdf


def subset_arrays_to_valid_directions(
    vp_direction_array: np.ndarray,
    vp_geometry: shapely.LineString,
    vp_idx_array: np.ndarray,
    stop_geometry: shapely.Point,
    stop_direction: str,
) -> np.ndarray: 
    """
    Each row stores several arrays related to vp.
    vp_direction is an array, vp_idx is an array,
    and the linestring of vp coords can be coerced into an array.
    
    When we're doing nearest neighbor search, we want to 
    first filter the full array down to valid vp
    before snapping it.
    """
    N_NEAREST_POINTS = 10
    
    opposite_direction = vp_transform.OPPOSITE_DIRECTIONS[stop_direction] 
    
    # These are the valid index values where opposite direction 
    # is excluded       
    valid_indices = (vp_direction_array != opposite_direction).nonzero()   

    vp_coords_line = np.array(vp_geometry.coords)[valid_indices]
    
    vp_idx_arr = np.asarray(vp_idx_array)[valid_indices]  
            
    np_inds = geo_utils.nearest_snap(
        vp_coords_line, stop_geometry, N_NEAREST_POINTS
    )
        
    # nearest neighbor returns self.N 
    # if there are no nearest neighbor results found
    # if we want 10 nearest neighbors and 8th, 9th, 10th are all
    # the same result, the 8th will have a result, then 9th and 10th will
    # return the length of the array (which is out-of-bounds)
    np_inds2 = np_inds[np_inds < vp_idx_arr.size]
    
    nearest_vp_arr = vp_idx_arr[np_inds2]
    
    return nearest_vp_arr


def add_nearest_neighbor_result_array(
    gdf: gpd.GeoDataFrame, 
    analysis_date: str,
    **kwargs
) -> pd.DataFrame:
    """
    Add the nearest k_neighbors result.
    """    
    nearest_vp_arr_series = []
    
    for row in gdf.itertuples():
        
        nearest_vp_arr = subset_arrays_to_valid_directions(
            getattr(row, "vp_primary_direction"),
            getattr(row, "vp_geometry"),
            getattr(row, "vp_idx"),
            getattr(row, "stop_geometry"),
            getattr(row, "stop_primary_direction"),
        )
        
        nearest_vp_arr_series.append(nearest_vp_arr)
        
    gdf2 = gdf.assign(
        nearest_vp_arr = nearest_vp_arr_series
    ).drop(columns = ["vp_primary_direction", "vp_idx", "vp_geometry"])
    
    return gdf2