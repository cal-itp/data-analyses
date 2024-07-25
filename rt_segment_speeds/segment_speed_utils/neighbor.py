"""
Nearest neighbor utility functions.
"""
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from scipy.spatial import KDTree

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import gtfs_schedule_wrangling, wrangle_shapes     
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT

# Could we use distance to filter for nearest neighbor?
# It can make the length of results more unpredictable...maybe we stick to 
# k_neighbors and keep the nearest k, so that we can at least be 
# more consistent with the arrays returned
geo_const_meters = 6_371_000 * np.pi / 180
geo_const_miles = 3_959_000 * np.pi / 180

def nearest_snap(
    line: shapely.LineString, 
    point: shapely.Point,
    k_neighbors: int = 1
) -> np.ndarray:
    """
    Based off of this function,
    but we want to return the index value, rather than the point.
    https://github.com/UTEL-UIUC/gtfs_segments/blob/main/gtfs_segments/geom_utils.py
    """
    line = np.asarray(line.coords)
    point = np.asarray(point.coords)
    tree = KDTree(line)
    
    # np_dist is array of distances of result (let's not return it)
    # np_inds is array of indices of result
    _, np_inds = tree.query(
        point, workers=-1, k=k_neighbors, 
    )
    
    return np_inds.squeeze()
    

def add_nearest_vp_idx(
    vp_linestring: shapely.LineString, 
    stop: shapely.Point, 
    vp_idx_arr: np.ndarray
) -> int:
    """
    Index into where the nearest vp is to the stop,
    and return that vp_idx value from the vp_idx array.
    """
    idx = nearest_snap(vp_linestring, stop, k_neighbors=1)
    
    return vp_idx_arr[idx]

    
def merge_stop_vp_for_nearest_neighbor(
    stop_times: gpd.GeoDataFrame,
    analysis_date: str,
    **kwargs
) -> gpd.GeoDataFrame:
    VP_NN = GTFS_DATA_DICT.speeds_tables.vp_nearest_neighbor
    
    vp_condensed = gpd.read_parquet(
        f"{SEGMENT_GCS}{VP_NN}_{analysis_date}.parquet",
        columns = ["trip_instance_key", 
                   "vp_idx", "vp_primary_direction", 
                   "geometry"],
        **kwargs
    ).to_crs(WGS84)

    gdf = pd.merge(
        stop_times.rename(
            columns = {
                "geometry": "stop_geometry"}
        ).set_geometry("stop_geometry").to_crs(WGS84),
        vp_condensed.rename(
            columns = {
                "vp_primary_direction": "stop_primary_direction",
                "geometry": "vp_geometry"
            }),
        on = ["trip_instance_key", "stop_primary_direction"],
        how = "inner"
    )
        
    return gdf


def add_nearest_neighbor_result_array(
    gdf: gpd.GeoDataFrame, 
    analysis_date: str,
    **kwargs
) -> pd.DataFrame:
    """
    Add the nearest k_neighbors result.
    """
    N_NEAREST_POINTS = 10
    
    nearest_vp_arr_series = []
    
    for row in gdf.itertuples():
        vp_coords_line = getattr(row, "vp_geometry")
        stop_geometry = getattr(row, "stop_geometry")
        vp_idx_arr = getattr(row, "vp_idx")
        
        np_inds = nearest_snap(
            vp_coords_line, stop_geometry, N_NEAREST_POINTS
        )
        
        # nearest neighbor returns self.N 
        # if there are no nearest neighbor results found
        # if we want 10 nearest neighbors and 8th, 9th, 10th are all
        # the same result, the 8th will have a result, then 9th and 10th will
        # return the length of the array (which is out-of-bounds)
        
        np_inds2 = np_inds[np_inds < vp_idx_arr.size]
        
        nearest_vp_arr = vp_idx_arr[np_inds2]
        
        nearest_vp_arr_series.append(nearest_vp_arr)
    
    gdf2 = gdf.assign(
        nearest_vp_arr = nearest_vp_arr_series
    ).drop(columns = ["vp_idx", "vp_geometry"])
    
    return gdf2