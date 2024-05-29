"""
Run nearest_vp_to_stop.py, 
interpolate_stop_arrivals.py,
and calculate_speed_from_stop_arrivals.py for road_segments.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from shared_utils import dask_utils
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import SEGMENT_TYPES

from calitp_data_analysis import utils
from segment_speed_utils import neighbor
#from nearest_vp_to_stop import nearest_neighbor_for_stop
#from interpolate_stop_arrival import interpolate_stop_arrivals
#from stop_arrivals_to_speed import calculate_speed_from_stop_arrivals

segment_type = "road_segments"

def export_results(
    results: pd.DataFrame,
    i,
    analysis_date,
    EXPORT_FILE
):
    
    if len(results) > 0:
        results.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}_staging/"
            f"batch_{i}.parquet",
        )
        del results
    
    return

def nearest_neighbor_one_operator(
    analysis_date: str, 
    one_operator: str,
    dict_inputs
):
    PROXY_STOP_TIMES = dict_inputs.proxy_stop_times
    
    stop_times = gpd.read_parquet(
        f"{SEGMENT_GCS}{PROXY_STOP_TIMES}_{analysis_date}",
            filters = [[("schedule_gtfs_dataset_key", "==", one_operator)]]
    ).rename(
        columns = {"primary_direction": "stop_primary_direction"}
    ) 
    
    subset_trips = stop_times.trip_instance_key.unique()
    
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(
        stop_times,
        analysis_date,
        filters = [[("trip_instance_key", "in", subset_trips)]]
    )
    
    results = neighbor.add_nearest_neighbor_result(
        gdf, 
        analysis_date,
        filters = [[("trip_instance_key", "in", subset_trips)]]
    )
    

    return results

def nearest_neighbor_all_operators(
    analysis_date: str, 
    dict_inputs
):
    PROXY_STOP_TIMES = dict_inputs.proxy_stop_times
    
    stop_times = gpd.read_parquet(
        f"{SEGMENT_GCS}{PROXY_STOP_TIMES}_{analysis_date}",
    ).rename(
        columns = {"primary_direction": "stop_primary_direction"}
    ) 
        
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(
        stop_times,
        analysis_date,
    )
    
    results = neighbor.add_nearest_neighbor_result(
        gdf, 
        analysis_date,
    ).pipe(only_valid_nn)
    
    return results

def only_valid_nn(results):
    trip_stop_cols = ["trip_instance_key", "linearid", "mtfcc", "segment_sequence"]

    min_vp_idx = (results.groupby(trip_stop_cols)
                  .agg({"nearest_vp_idx": "min"})
                  .reset_index()
                  .rename(columns = {"nearest_vp_idx": "min_vp_idx"})
    )    
    max_vp_idx = (results.groupby(trip_stop_cols)
                  .agg({"nearest_vp_idx": "max"})
                  .reset_index()
                  .rename(columns = {"nearest_vp_idx": "max_vp_idx"})
    )
    
    valid_nn = pd.merge(
        min_vp_idx,
        max_vp_idx,
        on = trip_stop_cols,
        how = "inner"
    ).query('min_vp_idx != max_vp_idx')[trip_stop_cols]
    
    results2 = pd.merge(
        results,
        valid_nn,
        on = trip_stop_cols,
        how = "inner"
    )
    
    return results2


def nearest_neighbor_for_road(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    segment_type == "road_segments"

    dict_inputs = config_path[segment_type]
    EXPORT_FILE = dict_inputs["stage2"]

    PROXY_STOP_TIMES = dict_inputs.proxy_stop_times
    '''
    operators = pd.read_parquet(
        f"{SEGMENT_GCS}{PROXY_STOP_TIMES}_{analysis_date}",
        columns = ["schedule_gtfs_dataset_key"]
    ).schedule_gtfs_dataset_key.unique()
    
    operators = sorted(operators)
    
    result_dfs = [
        delayed(nearest_neighbor_one_operator)(
            analysis_date, one_operator, dict_inputs
        ).pipe(only_valid_nn) 
        for one_operator in operators 
    ]
    
    result_ddf = dd.from_delayed(result_dfs)
    result_ddf = 
    
    writes = [
        delayed(export_one_operator)(i, analysis_date, EXPORT_FILE) 
        for i in result_dfs
    ]
    dd.compute(*writes)
    '''    
    results = delayed(nearest_neighbor_all_operators)(analysis_date, dict_inputs)

    gddf = dd.from_delayed(results)
    gddf = gddf.repartition(partition_size="75MB")
    partitions = gddf.to_delayed()
    writes = [
        delayed(export_results)(
            part, i, analysis_date, EXPORT_FILE)
        for i, part in enumerate(partitions)
    ]
    
    dd.compute(*writes)
    '''
    print("merge stop with nn")    
    
    gdf_dtypes = gddf.dtypes.to_dict()
    
    results_ddf = gddf.map_partitions(
        neighbor.add_nearest_neighbor_result,
        analysis_date,
        meta = {
            **gdf_dtypes,
            "nearest_vp_idx": "int",
            "vp_idx_trio": "object",
            "location_timestamp_local_trio": "object",
            "vp_coords_trio": "geometry"
        },
        align_dataframes = False
    )

    print("nearest neighbor results")
    '''
    
    return

if __name__ == "__main__":
        
    from segment_speed_utils.project_vars import analysis_date_list
    
    print(f"segment_type: {segment_type}")
    
    for analysis_date in analysis_date_list:      
                        
        LOG_FILE = "../logs/nearest_vp.log"
        logger.add(LOG_FILE, retention="3 months")
        logger.add(sys.stderr, 
                   format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
                   level="INFO")
    
        nearest_neighbor_for_road(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        )    
        '''
        LOG_FILE = "../logs/interpolate_stop_arrival.log"
        logger.add(LOG_FILE, retention="3 months")
        logger.add(sys.stderr, 
                   format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
                   level="INFO")

        interpolate_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )
        
        
        LOG_FILE = "../logs/speeds_by_segment_trip.log"
        logger.add(LOG_FILE, retention="3 months")
        logger.add(sys.stderr, 
                   format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
                   level="INFO")

        calculate_speed_from_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        )
        '''
