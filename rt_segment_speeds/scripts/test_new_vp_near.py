import datetime
import sys

from dask import delayed, compute
from loguru import logger

from update_vars import GTFS_DATA_DICT, SEGMENT_GCS

# Add me for this script
from calitp_data_analysis import utils
from typing import Literal, Optional
from segment_speed_utils.project_vars import SEGMENT_TYPES
from segment_speed_utils import neighbor
from pathlib import Path
from nearest_vp_to_stop import stop_times_for_shape_segments

def nearest_neighbor_for_stop(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    """
    Set up nearest neighbors for RT stop times, which
    includes all trips. Use stop sequences for each trip.
    """
    start = datetime.datetime.now()

    dict_inputs = config_path[segment_type]
    
    EXPORT_FILE = f'{dict_inputs["stage2"]}_{analysis_date}_test'
    trip_stop_cols = [*dict_inputs["trip_stop_cols"]]
    
    stop_time_col_order = [
        'trip_instance_key', 'shape_array_key',
        'stop_sequence', 'stop_id', 'stop_pair',
        'stop_primary_direction', 'geometry'
    ] 
    
    if segment_type == "stop_segments":
        stop_times = stop_times_for_shape_segments(analysis_date, dict_inputs)
        stop_times = stop_times.reindex(columns = stop_time_col_order)
    
    else:
        print(f"{segment_type} is not valid")
    
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(
        stop_times, analysis_date)
        
    results = neighbor.add_nearest_neighbor_result_array2(gdf, analysis_date)
          
    # Keep columns from results that are consistent across segment types 
    # use trip_stop_cols as a way to uniquely key into a row 
    keep_cols = trip_stop_cols + [
        "shape_array_key",
        "stop_geometry",
        "nearest_vp_arr"
    ]
    
    utils.geoparquet_gcs_export(
        results[keep_cols],
        SEGMENT_GCS,
        EXPORT_FILE,
    )
    
    end = datetime.datetime.now()
    logger.info(f"nearest neighbor for {segment_type} "
                f"{analysis_date}: {end - start}")
    
    del gdf, stop_times, results

    return

if __name__ == "__main__":
    
    #from segment_speed_utils.project_vars import analysis_date_list    
    from shared_utils import rt_dates
    analysis_date_list = [rt_dates.DATES["oct2024"]]
    
    segment_type = "stop_segments"
    print(f"segment_type: {segment_type}")
    
    LOG_FILE = "../logs/nearest_vp.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    delayed_dfs = [
        delayed(nearest_neighbor_for_stop)(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        ) for analysis_date in analysis_date_list
    ]

    [compute(i)[0] for i in delayed_dfs]
    
    del delayed_dfs