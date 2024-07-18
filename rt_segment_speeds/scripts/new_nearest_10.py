import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from scipy.spatial import KDTree

from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers#, neighbor
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import rt_dates

analysis_date = rt_dates.DATES["apr2024"]

import nearest_vp_to_stop

def get_subset_trips(analysis_date: str) -> list:
    bbb_trips = helpers.import_scheduled_trips(
        analysis_date,
        filters = [("name", "==", "Big Blue Bus Schedule")],
        columns = ["gtfs_dataset_key", "trip_instance_key"],
        get_pandas = True
    )

    bbb_key = bbb_trips.schedule_gtfs_dataset_key.iloc[0]
    subset_trips = bbb_trips.trip_instance_key.unique()

    return subset_trips

def construct_stop_times(
    analysis_date: str, 
    subset_trips: list
) -> gpd.GeoDataFrame:

    # Grab the relevant stop times rows
    # will need to concatenate RT stop times (all trips) 
    # with additional segments for speedmaps
    rt_stop_times = (
        nearest_vp_to_stop.stop_times_for_all_trips(analysis_date)
        .query('trip_instance_key in @subset_trips')
    )

    proxy_stop_times = (
        nearest_vp_to_stop.stop_times_for_speedmaps(analysis_date)
        .query('trip_instance_key in @subset_trips')
    )

    bbb_stop_times = pd.concat(
        [rt_stop_times, proxy_stop_times], 
        axis=0, ignore_index=True
    )
    
    return bbb_stop_times

def merge_stop_vp_for_nearest_neighbor(
    stop_times: gpd.GeoDataFrame,
    analysis_date: str,
    **kwargs
) -> gpd.GeoDataFrame:
    
    vp_condensed = gpd.read_parquet(
        f"{SEGMENT_GCS}condensed/"
        f"vp_nearest_neighbor_dwell_{analysis_date}.parquet",
        **kwargs
    ).to_crs(PROJECT_CRS)

    gdf = pd.merge(
        stop_times.rename(
            columns = {
                "geometry": "stop_geometry"}
        ).set_geometry("stop_geometry").to_crs(PROJECT_CRS),
        vp_condensed.rename(
            columns = {
                "vp_primary_direction": "stop_primary_direction",
                "geometry": "vp_geometry"
            }),
        on = ["trip_instance_key", "stop_primary_direction"],
        how = "inner"
    )
        
    return gdf

def nearest_snap(
    line: shapely.LineString, 
    point: shapely.Point,
    k_neighbors: int
) -> int:
    """
    Based off of this function,
    but we want to return the index value, rather than the point.
    https://github.com/UTEL-UIUC/gtfs_segments/blob/main/gtfs_segments/geom_utils.py
    """
    line = np.asarray(line.coords)
    point = np.asarray(point.coords)
    tree = KDTree(line)
    
    # np_dist is array of distances of result
    # np_inds is array of indices of result
    np_dist, np_inds = tree.query(
        point, workers=-1, k=k_neighbors, 
    )
    
    return np_dist.squeeze(), np_inds.squeeze()
    

if __name__ == "__main__":

    start = datetime.datetime.now()
    
    subset_trips = get_subset_trips(analysis_date)

    bbb_stop_times = construct_stop_times(analysis_date, subset_trips)
    
    # This is with opposite direction removed
    gdf = merge_stop_vp_for_nearest_neighbor(
        bbb_stop_times,
        analysis_date,
        filters = [[("trip_instance_key", "in", subset_trips)]],
        # just keep columns for merge + vp_idx and vp_geometry 
        columns = [
            "trip_instance_key", "vp_idx", 
            "vp_primary_direction", "geometry"
        ], 
    )
    
    N_NEAREST_POINTS = 10
    
    nearest_vp_arr_series = []
    
    for row in gdf.itertuples():
        vp_coords_line = getattr(row, "vp_geometry")
        stop_geometry = getattr(row, "stop_geometry")
        vp_idx_arr = getattr(row, "vp_idx")
        
        _, np_inds = nearest_snap(
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
    
    utils.geoparquet_gcs_export(
        gdf2,
        SEGMENT_GCS,
        f"nearest/test_nearest_vp_to_stop_{analysis_date}"
    )
        
    end = datetime.datetime.now()
    print(f"save nearest 10 (BBB): {end - start}")