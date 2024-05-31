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

from calitp_data_analysis import utils
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import SEGMENT_TYPES

from calitp_data_analysis import utils
from segment_speed_utils import neighbor
#from nearest_vp_to_stop import nearest_neighbor_for_stop
#from interpolate_stop_arrival import interpolate_stop_arrivals
#from stop_arrivals_to_speed import calculate_speed_from_stop_arrivals

segment_type = "road_segments"

def export_operator_results(
    results: pd.DataFrame,
    i: int,
    analysis_date,
    EXPORT_FILE: str
):
    #if len(results) > 0:
        #gtfs_key = results.schedule_gtfs_dataset_key.iloc[0]
        
        #print(f"{gtfs_key}: {len(results)}")

    utils.geoparquet_gcs_export(
        results,
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}_staging2/",
        f"batch{i}"
    )

    del results
    
    return


def setup_nearest_neighbor_one_operator(
    analysis_date: str, 
    dict_inputs: dict, 
    one_operator: str
) -> gpd.GeoDataFrame:
    
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
    
    return gdf


def nn_result_one_operator(
    gdf: gpd.GeoDataFrame, 
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Add filter for trips present for the operator before adding nearest 
    neighbor result.
    """
    subset_trips = gdf.trip_instance_key.unique().tolist()
    
    results = neighbor.add_nearest_neighbor_result(
        gdf, 
        analysis_date,
        filters = [[("trip_instance_key", "in", subset_trips)]]
    )
    
    return results


def filter_to_valid_nn(results: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Only keep nearest neighbor rows where the origin/destination stops 
    for the segment actually show different vp_idx values.
    If it's the same, then no speed would be calculated anyway.
    """
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


def chunk_data(df: gpd.GeoDataFrame, n: int) -> list:
    """
    Subset results by a chunk size to ensure that we
    don't crash kernel when exporting.
    """
    # https://stackoverflow.com/questions/33367142/split-dataframe-into-relatively-even-chunks-according-to-length
    list_df = [df[i:i+n] for i in range(0, df.shape[0], n)]

    return list_df


def set_new_division(ddf, chunk_rows: int):
    cumlens = ddf.map_partitions(len).compute().cumsum()
    
    # since processing will be done on a partition-by-partition basis, save them
    # individually
    new_partitions = [ddf.partitions[0]]
    for npart, partition in enumerate(ddf.partitions[1:].partitions):
        partition.index = partition.index + cumlens[npart]
        new_partitions.append(partition)

    # this is our new ddf
    ddf = dd.concat(new_partitions)
    
    #  set divisions based on cumulative lengths
    ddf.divisions = tuple([0] + cumlens.tolist())

    # change the divisions to have the desired spacing
    max_rows = cumlens.tolist()[-1]
    new_divisions = list(range(0, max_rows, chunk_rows))
    
    if new_divisions[-1] < max_rows:
        new_divisions.append(max_rows)
    
    # now there will be desired rows per partition
    ddf2 = ddf.repartition(divisions=new_divisions)

    return ddf2


def nearest_neighbor_for_road(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    start = datetime.datetime.now()
    
    segment_type == "road_segments"

    dict_inputs = config_path[segment_type]
    EXPORT_FILE = dict_inputs["stage2"]

    PROXY_STOP_TIMES = dict_inputs.proxy_stop_times

    operators = pd.read_parquet(
        f"{SEGMENT_GCS}{PROXY_STOP_TIMES}_{analysis_date}",
        columns = ["schedule_gtfs_dataset_key"]
    ).schedule_gtfs_dataset_key.unique().tolist()
    
    stop_time_dfs = [
        delayed(setup_nearest_neighbor_one_operator)(
            analysis_date, dict_inputs, one_operator) 
        for one_operator in operators
    ]  
    
    nn_gdfs = [
        delayed(nn_result_one_operator)(
            df, analysis_date)
        for df in stop_time_dfs
    ]

    result_dfs = [
        delayed(filter_to_valid_nn)(
            df)
        for df in nn_gdfs
    ]
    
    #orig_dtypes = gddf.dtypes.to_dict()
    nn_dtypes = {
        "nearest_vp_idx": "int",
        "vp_idx_trio": "object",
        "location_timestamp_local_trio": "object",
        "vp_coords_trio": "geometry"
    }
    
    results_ddf = dd.from_delayed(result_dfs)
        
    time1 = datetime.datetime.now()
    logger.info(f"all delayed dfs: {time1 - start}")

    results_ddf = results_ddf.repartition(npartitions=150)
    
    time2 = datetime.datetime.now()
    logger.info(f"chunks: {time2 - time1}")
    
    writes = [
        delayed(export_operator_results)(
            results_ddf.partitions[i], i, 
            analysis_date, EXPORT_FILE) 
        for i in range(0, results_ddf.npartitions)
    ]
    
    time3 = datetime.datetime.now()
    logger.info(f"get chunks: {time3- time2}")
    
    ''' 
    nn_result_dfs = [
        delayed(nn_result_one_operator)(df, analysis_date) 
        for df in stop_time_dfs
    ]
 
    print("get nn")

    result_dfs = [
        delayed(filter_to_valid_nn)(df) 
        for df in nn_result_dfs
    ]
    
    print("filter to valid nn")
    
    time1 = datetime.datetime.now()
    logger.info(f"all delayed dfs: {time1 - start}")
    
    results_ddf = dd.from_delayed(results)
    
    time2 = datetime.datetime.now()
    logger.info(f"delayed_dfs to ddf: {time2 - time1}")
    
    # chunk results into x rows?
    N_CHUNK_ROWS = 1_000_000
    
    # https://stackoverflow.com/questions/65832338/process-dask-dataframe-by-chunks-of-rows
    results_chunked = set_new_division(results_ddf, N_CHUNK_ROWS)
    
    time3 = datetime.datetime.now()
    logger.info(f"ddf new divisions: {time3 - time2}")
    
    writes = [
        delayed(export_operator_results)(
            results_chunked.partitions[i], i, 
            analysis_date, EXPORT_FILE) 
        for i in range(0, results_chunked.npartitions)
    ]
    '''
    dd.compute(*writes)
    
    end = datetime.datetime.now()
    logger.info(f"export batches: {end - time3}")
    logger.info(f"execution time: {end - time3}")
    
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
