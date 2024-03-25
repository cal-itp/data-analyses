import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from scipy.spatial import cKDTree

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import gtfs_schedule_wrangling, wrangle_shapes     
from segment_speed_utils.project_vars import SEGMENT_GCS

geo_const_meters = 6_371_000 * np.pi / 180
geo_const_miles = 3_959_000 * np.pi / 180

def nearest_snap(line: shapely.LineString, point: shapely.Point) -> int:
    """
    Based off of this function,
    but we want to return the index value, rather than the point.
    https://github.com/UTEL-UIUC/gtfs_segments/blob/main/gtfs_segments/geom_utils.py
    """
    line = np.asarray(line.coords)
    point = np.asarray(point.coords)
    tree = cKDTree(line)
    
    # np_dist is array of distances of result
    # np_inds is array of indices of result
    # to get approx distance in meters: geo_const * np_dist
    _, np_inds = tree.query(
        point, workers=-1, k=1, 
        #distance_upper_bound = geo_const_miles * 5 # upper bound of 5 miles
    )
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
    array_length = len(vp_idx_arr)
    try:
        start_idx = np.where(vp_idx_arr == nearest_value)[0][0]
    
    # Just in case we don't have any vp_idx_arr, set to zero and
    # use condition later to return empty array
    except:
        start_idx = 0

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
    elif (start_idx == array_length - 1) and (array_length > 0):
        start_pos = start_idx - 2
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
    analysis_date: str
) -> gpd.GeoDataFrame:
    
    vp_condensed = gpd.read_parquet(
        f"{SEGMENT_GCS}condensed/"
        f"vp_nearest_neighbor_{analysis_date}.parquet",
    ).drop(columns = "location_timestamp_local").to_crs(WGS84)
        
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


def add_nearest_neighbor_result(
    gdf: gpd.GeoDataFrame, 
    analysis_date: str
) -> pd.DataFrame:
    """
    Add the nearest vp_idx. Also add and trio of be the boundary
    of nearest_vp_idx. Trio provides the vp_idx, timestamp,
    and vp coords we need to do stop arrival interpolation.
    """
    # Grab vp_condensed, which contains all the coords for entire trip
    vp_full = gpd.read_parquet(
        f"{SEGMENT_GCS}condensed/vp_condensed_{analysis_date}.parquet",
        columns = ["trip_instance_key", "vp_idx", 
                   "location_timestamp_local", 
                   "geometry"],
    ).rename(columns = {
        "vp_idx": "trip_vp_idx",
        "geometry": "trip_geometry"
    }).set_geometry("trip_geometry").to_crs(WGS84)
    
    gdf2 = pd.merge(
        gdf,
        vp_full,
        on = "trip_instance_key",
        how = "inner"
    )
        
    nearest_vp_idx_series = []    
    vp_trio_series = []
    time_trio_series = []
    coords_trio_series = []
    
    # Iterate through and find the nearest_vp_idx, then surrounding trio
    nearest_vp_idx = np.vectorize(add_nearest_vp_idx)( 
        gdf2.vp_geometry, gdf2.stop_geometry, gdf2.vp_idx
    )
        
    gdf2 = gdf2.assign(
        nearest_vp_idx = nearest_vp_idx,
    ).drop(
        columns = ["vp_idx", "vp_geometry"]
    )
    
    for row in gdf2.itertuples():
        vp_trio, time_trio, coords_trio = add_trio(
            getattr(row, "nearest_vp_idx"), 
            np.asarray(getattr(row, "trip_vp_idx")),
            np.asarray(getattr(row, "location_timestamp_local")),
            np.asarray(getattr(row, "trip_geometry").coords),
        )
        
        vp_trio_series.append(vp_trio)
        time_trio_series.append(time_trio)
        coords_trio_series.append(shapely.LineString(coords_trio))
                
    drop_cols = [
        "location_timestamp_local",
        "trip_vp_idx", "trip_geometry"
    ]
    
    gdf2 = gdf2.assign(
        vp_idx_trio = vp_trio_series,
        location_timestamp_local_trio = time_trio_series,
        vp_coords_trio = gpd.GeoSeries(coords_trio_series, crs = WGS84)
    ).drop(columns = drop_cols)
        
    return gdf2