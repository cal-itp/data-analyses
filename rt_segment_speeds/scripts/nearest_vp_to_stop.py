"""
Find nearest_vp_idx to the stop position 
using scipy KDTree.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import helpers, neighbor
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import SEGMENT_TYPES
                                              
    
def stop_times_for_shape_segments(
    analysis_date: str,
    dict_inputs: dict
) -> gpd.GeoDataFrame:
    """
    This is the stop times table using only 1 shape for each 
    route-direction. Every trip belong to that shape
    will be cut along the same stops.
    This allows us to aggregate segments across trips because each 
    segment has the same stop_id1 and stop_id2.
    """
    SEGMENT_FILE = dict_inputs["segments_file"]
    
    rt_trips = helpers.import_unique_vp_trips(analysis_date)

    shape_stop_combinations = pd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}_{analysis_date}.parquet",
        columns = ["trip_instance_key",
                   "stop_id1", "stop_pair",
                   "st_trip_instance_key"],
        filters = [[("trip_instance_key", "in", rt_trips)]]
    ).rename(columns = {"stop_id1": "stop_id"})
    
    subset_trips = shape_stop_combinations.st_trip_instance_key.unique()
    
    stops_to_use = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key",
                   "stop_sequence", "stop_id", "stop_pair",
                   "stop_primary_direction", "geometry"],
        filters = [[("trip_instance_key", "in", subset_trips)]],
        get_pandas = True,
        with_direction = True
    ).rename(columns = {"trip_instance_key": "st_trip_instance_key"})
    
    stop_times = pd.merge(
        stops_to_use,
        shape_stop_combinations,
        on = ["st_trip_instance_key", "stop_id", "stop_pair"],
        how = "inner"
    ).drop(
        columns = "st_trip_instance_key"
    ).drop_duplicates().reset_index(drop=True)
    
    return stop_times


def stop_times_for_all_trips(
    analysis_date: str,
) -> gpd.GeoDataFrame:
    """
    This is the stop times table for all trips.
    We will do nearest neighbors for every stop along a trip.
    """
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key",
                   "stop_sequence", "stop_id", "stop_pair", 
                   "stop_primary_direction",
                   "geometry"],
        with_direction = True,
        get_pandas = True,
        crs = WGS84
    )
    
    return stop_times


def nearest_neighbor_for_stop(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    """
    Set up nearest neighbors for RT stop times, which
    includes all trips. Use stop sequences for each trip.
    """
    
    dict_inputs = config_path[segment_type]
    
    start = datetime.datetime.now()
    EXPORT_FILE = f'{dict_inputs["stage2"]}'
    
    stop_time_col_order = [
        'trip_instance_key', 'shape_array_key',
        'stop_sequence', 'stop_id', 'stop_pair',
        'stop_primary_direction', 'geometry'
    ] 
    
    if segment_type == "stop_segments":
        stop_times = stop_times_for_shape_segments(analysis_date, dict_inputs)
    
    elif segment_type == "rt_stop_times":
        stop_times = stop_times_for_all_trips(analysis_date)
    
    else:
        print(f"{segment_type} is not valid")
    
    stop_times = stop_times.reindex(columns = stop_time_col_order)
    
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(
        stop_times, analysis_date)
        
    results = neighbor.add_nearest_neighbor_result(gdf, analysis_date)
    
    utils.geoparquet_gcs_export(
        results,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}",
    )
    
    
    end = datetime.datetime.now()
    logger.info(f"nearest neighbor for {segment_type} "
                f"{analysis_date}: {end - start}")
    
    return
    
if __name__ == "__main__":
        
    LOG_FILE = "../logs/nearest_vp.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO") 
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    for analysis_date in analysis_date_list:
        nearest_neighbor_for_stop(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        ) 