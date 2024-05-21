import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger
from pathlib import Path
from typing import Optional

from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from calitp_data_analysis import utils
from segment_speed_utils import neighbor
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
    
    STOP_TIMES_FILE = GTFS_DATA_DICT.speedmap_segments.proxy_stop_times

    stop_times = gpd.read_parquet(
        f"{SEGMENT_GCS}{STOP_TIMES_FILE}_{analysis_date}.parquet",
        filters = [[("proxy_stop", "==", 1)]]
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


def concatenate_speedmap_proxy_arrivals_with_remaining(
    analysis_date: str,
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    """
    Nearest vp and interpolation was done just for extra
    speedmap segments.
    
    Only 6% of segments had extra segments / cutpoints 
    (proxy stops), so we need not run the entire nearest neighbor
    redundantly. We can do nearest neighbor on just those 6% 
    and concatenate the full results from rt_stop_times pipeline, 
    which is every trip-stop anyway.
    
    Append those results and all the stop arrivals into 
    speed calculation.
    """
    PROXY_STOP_ARRIVALS_FILE = GTFS_DATA_DICT.speedmap_segments.stage3
    OTHER_STOP_ARRIVALS_FILE = GTFS_DATA_DICT.rt_stop_times.stage3
    SPEEDMAP_STOP_ARRIVALS = GTFS_DATA_DICT.speedmap_segments.stage3b
    trip_stop_cols = [*GTFS_DATA_DICT.speedmap_segments.trip_stop_cols]
    
    proxy_arrivals = pd.read_parquet(
        f"{SEGMENT_GCS}{PROXY_STOP_ARRIVALS_FILE}_{analysis_date}.parquet"
    )

    other_arrivals= pd.read_parquet(
        f"{SEGMENT_GCS}{OTHER_STOP_ARRIVALS_FILE}_{analysis_date}.parquet"
    )
        
    df = pd.concat([
        proxy_arrivals, 
        other_arrivals
    ], axis=0, ignore_index=True
    ).sort_values(
        trip_stop_cols
    ).reset_index(drop=True)
    
    df.to_parquet(
        f"{SEGMENT_GCS}{SPEEDMAP_STOP_ARRIVALS}_{analysis_date}.parquet"
    )
    
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
        
        start = datetime.datetime.now()
        
        segment_type = "speedmap_segments"
        
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
                  
        concatenate_speedmap_proxy_arrivals_with_remaining(
            analysis_date,
            config_path = GTFS_DATA_DICT
        )
        
        stop_arrivals_to_speed.calculate_speed_from_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )