import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger
from pathlib import Path
from typing import Optional, Literal, Union

from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from calitp_data_analysis import utils
from segment_speed_utils import helpers, neighbor
import interpolate_stop_arrival
import stop_arrivals_to_speed

def nearest_neighbor_for_stop(
    analysis_date: str,
    segment_type: str,
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
        'stop_sequence', 'stop_sequence1', 
        'stop_id', 'stop_pair',
        'stop_primary_direction', 'geometry'
    ] 
    
    # filter to Big Blue Bus
    subset_shapes = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["shape_array_key"],
        filters = [[("name", "==", "Big Blue Bus Schedule")]],           
        get_pandas=True
    ).dropna().shape_array_key.unique()
        
    
    stop_times = gpd.read_parquet(
        f"{SEGMENT_GCS}stop_time_expansion/"
        f"speedmap_stop_times_{analysis_date}.parquet",
        filters = [[
            ("shape_array_key", "in", subset_shapes)
        ]]
    )

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
    
    #from segment_speed_utils.project_vars import analysis_date_list
    from shared_utils import rt_dates
    
    for analysis_date in [rt_dates.DATES["mar2024"]]:
        segment_type = "speedmap_segments"
        '''
        nearest_neighbor_for_stop(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        ) 
        
        interpolate_stop_arrival.interpolate_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )
        '''
        stop_arrivals_to_speed.calculate_speed_from_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )