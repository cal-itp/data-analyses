"""
Find nearest_vp_idx to the stop position 
using scipy KDTree.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from loguru import logger
from pathlib import Path

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import helpers, neighbor
from segment_speed_utils.project_vars import SEGMENT_GCS


def add_nearest_neighbor_result(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add the nearest vp_idx. Also add and trio of be the boundary
    of nearest_vp_idx. Trio provides the vp_idx, timestamp,
    and vp coords we need to do stop arrival interpolation.
    """
    # Use direction=Unknown because no directions are excluded.
    vp_condensed = gpd.read_parquet(
        f"{SEGMENT_GCS}condensed/vp_condensed_{analysis_date}.parquet",
        columns = ["trip_instance_key", "vp_idx", 
                   "location_timestamp_local", 
                  "geometry"]
    ).rename(columns = {
        "vp_idx": "trip_vp_idx",
        "geometry": "trip_geometry"
    })
    
    gdf2 = pd.merge(
        gdf,
        vp_condensed,
        on = "trip_instance_key",
        how = "inner"
    )
    
    del vp_condensed
    
    # np.vectorize seems to work here for a loop
    nearest_vp_idx = np.vectorize(neighbor.add_nearest_vp_idx)( 
        gdf2.geometry, gdf2.stop_geometry, gdf2.vp_idx
    )
        
    gdf2 = gdf2.assign(
        nearest_vp_idx = nearest_vp_idx,
    ).drop(columns = ["vp_idx", "geometry"])
        
    vp_trio_series = []
    time_trio_series = []
    coords_trio_series = []
    
    # don't think np.vectorize works well for returning arrays...
    # need to find another method, so use itertuples here
    for row in gdf2.itertuples():
        nearest_value = getattr(row, "nearest_vp_idx")
        vp_idx_arr = np.asarray(getattr(row, "trip_vp_idx"))
        timestamp_arr = np.asarray(getattr(row, "location_timestamp_local"))
        coords_arr = np.asarray(getattr(row, "trip_geometry").coords)
                
        vp_trio, time_trio, coords_trio = neighbor.add_trio(
            nearest_value, vp_idx_arr, timestamp_arr, coords_arr)
        
        trio_line = shapely.LineString(coords_trio)
        
        vp_trio_series.append(vp_trio)
        time_trio_series.append(time_trio)
        coords_trio_series.append(trio_line)
        
    results = gdf2.assign(
        vp_idx_trio = vp_trio_series,
        location_timestamp_local_trio = time_trio_series,
        vp_coords_trio = gpd.GeoSeries(coords_trio_series, crs = WGS84)
    ).drop(columns = [
        "trip_vp_idx", 
        "location_timestamp_local", "trip_geometry"]
    )
    
    del gdf, gdf2
    
    return results
    

def nearest_neighbor_rt_stop_times(
    analysis_date: str,
    dict_inputs: dict
):
    """
    Set up nearest neighbors for RT stop times, which
    includes all trips. Use stop sequences for each trip.
    """
    start = datetime.datetime.now()
    EXPORT_FILE = Path(f'{dict_inputs["stage2"]}')
    
    # Unknown directions can be done separately for origin stop
    # we will use other methods to pare down the vp_idx 
    # maybe based on min(vp_idx) to use as the max bound
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}condensed/vp_nearest_neighbor_{analysis_date}.parquet",
    ).drop(columns = "location_timestamp_local")
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", 
                   "stop_sequence", "stop_id", "stop_pair", 
                   "stop_primary_direction",
                   "geometry"],
        with_direction = True,
        get_pandas = True,
        crs = WGS84
    )
        
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(stop_times, vp)
    del vp, stop_times
    
    results = add_nearest_neighbor_result(gdf)
    
    utils.geoparquet_gcs_export(
        results,
        f"{SEGMENT_GCS}{str(EXPORT_FILE.parent)}/",
        f"{EXPORT_FILE.stem}_{analysis_date}",
    )
    
    end = datetime.datetime.now()
    logger.info(f"RT stop times {analysis_date}: {end - start}")
    
    del results
    
    return


def nearest_neighbor_shape_segments(
    analysis_date: str,
    dict_inputs: dict
):
    """
    Set up nearest neighbors for segment speeds, which
    includes chooses 1 trip's stop sequences for that shape. 
    That trip, with stop sequences, stop ids from stop_times,
    is shared across all trips that use that shape_array_key. 
    """
    start = datetime.datetime.now()

    EXPORT_FILE = Path(f'{dict_inputs["stage2"]}')
    SEGMENT_FILE = dict_inputs["segments_file"]
    
    # Unknown directions can be done separately for origin stop
    # we will use other methods to pare down the vp_idx 
    # maybe based on min(vp_idx) to use as the max bound
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}condensed/vp_nearest_neighbor_{analysis_date}.parquet",
    ).drop(columns = "location_timestamp_local")
    
    subset_trips = pd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}_{analysis_date}.parquet",
        columns = ["st_trip_instance_key"]
    ).st_trip_instance_key.unique()
    
    stops_to_use = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key",
                    "stop_sequence", "stop_id", "stop_pair",
                   "stop_primary_direction",
                    "geometry"],
        filters = [[("trip_instance_key", "in", subset_trips)]],
        get_pandas = True,
        with_direction = True
    ).rename(columns = {"trip_instance_key": "st_trip_instance_key"})
    
    all_trips = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        get_pandas = True,
        with_direction = True
    ).drop_duplicates().reset_index(drop=True)
    
    stop_times = pd.merge(
        stops_to_use,
        all_trips,
        on = "shape_array_key",
        how = "inner"
    )
        
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(stop_times, vp)
    del vp, stop_times
    
    results = add_nearest_neighbor_result(gdf)

    utils.geoparquet_gcs_export(
        results,
        f"{SEGMENT_GCS}{str(EXPORT_FILE.parent)}/",
        f"{EXPORT_FILE.stem}_{analysis_date}",
    )
    
    end = datetime.datetime.now()
    logger.info(
        f"shape segments {analysis_date}: {end - start}")
    
    del results
    
    return 
    
    
if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list, CONFIG_PATH
    
    LOG_FILE = "../logs/nearest_vp.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    RT_STOP_TIMES_DICT = helpers.get_parameters(CONFIG_PATH, "rt_stop_times")
    
    for analysis_date in analysis_date_list:
        nearest_neighbor_shape_segments(analysis_date, STOP_SEG_DICT)
        #nearest_neighbor_rt_stop_times(analysis_date, RT_STOP_TIMES_DICT)
                               