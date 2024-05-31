"""
Run nearest_vp_to_stop.py, 
interpolate_stop_arrivals.py,
and calculate_speed_from_stop_arrivals.py for road_segments.
"""
import dask.dataframe as dd
import dask_geopandas as dg
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
from segment_speed_utils import helpers, neighbor
#from nearest_vp_to_stop import nearest_neighbor_for_stop
#from interpolate_stop_arrival import interpolate_stop_arrivals
#from stop_arrivals_to_speed import calculate_speed_from_stop_arrivals

segment_type = "road_segments"

def export_operator_results(
    results: pd.DataFrame,
    batch_i: int,
    partition_number: int,
    analysis_date,
    EXPORT_FILE: str
):
    
    utils.geoparquet_gcs_export(
        results,
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}_batch{batch_i}/",
        f"part{partition_number}.parquet"
    )

    del results
    
    return


def setup_nearest_neighbor_one_operator(
    analysis_date: str, 
    dict_inputs: dict, 
    operator_list: list
) -> gpd.GeoDataFrame:
    
    PROXY_STOP_TIMES = dict_inputs.proxy_stop_times
    
    stop_times = gpd.read_parquet(
        f"{SEGMENT_GCS}{PROXY_STOP_TIMES}_{analysis_date}",
            filters = [[("schedule_gtfs_dataset_key", "in", operator_list)]]
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


def filter_to_valid_nn(results: dg.GeoDataFrame) -> pd.DataFrame:
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
    
    valid_nn = dd.merge(
        min_vp_idx,
        max_vp_idx,
        on = trip_stop_cols,
        how = "inner"
    ).query('min_vp_idx != max_vp_idx')[trip_stop_cols]
    
    valid_nn = valid_nn.repartition(npartitions=1)
    
    results2 = dd.merge(
        results,
        valid_nn,
        on = trip_stop_cols,
        how = "inner"
    )
    
    return results2

def determine_batches(analysis_date: str) -> dict:
    operators = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "name"],
        get_pandas = True
    )
    
    rt_names = operators.name.unique().tolist()
    rt_keys = operators.schedule_gtfs_dataset_key.unique().tolist()
    
    #https://stackoverflow.com/questions/4843158/how-to-check-if-a-string-is-a-substring-of-items-in-a-list-of-strings
    
    la_metro_names = [
        "LA Metro Bus",
        "LA Metro Rail",
    ]
    
    other_large = [
        "Big Blue", 
        "Long Beach"
    ]
    
    bay_area_large_names = [
        "AC Transit", 
        "Muni"
    ]
    
    bay_area_names = [
        "Bay Area 511"
    ]

    # If any of the large operator name substring is 
    # found in our list of names, grab those
    # be flexible bc "Vehicle Positions" and "VehiclePositions" present
    matching1 = [k for n, k in zip(rt_names, rt_keys)
                if any(name in n for name in la_metro_names)]
    
    matching2 = [k for n, k in zip(rt_names, rt_keys)
                if any(name in n for name in bay_area_large_names)]  
    
    matching3 = [k for n, k in zip(rt_names, rt_keys)
                 if any(name in n for name in other_large)]
    
    remaining_bay_area = [k for n, k in zip(rt_names, rt_keys) 
                          if any(name in n for name in bay_area_names) and 
                          (k not in matching1) and (k not in matching2) and 
                          (k not in matching3)
                         ]
    
    remaining = [k for n, k in zip(rt_names, rt_keys) if 
                 (k not in matching1) and (k not in matching2) and 
                 (k not in matching3) and
                 (k not in remaining_bay_area)]
    
    def get_single_key(df, substring):
        return df[
            df.name.str.contains(substring)
        ].schedule_gtfs_dataset_key.tolist()
    
    # Batch large operators together and run remaining in 2nd query
    batch_dict = {}
    
    batch_dict[0] = get_single_key(operators, "LA Metro Bus")
    batch_dict[1] = get_single_key(operators, "LA Metro Rail")
    batch_dict[2] = get_single_key(operators, "AC Transit")
    batch_dict[3] = get_single_key(operators, "Muni")
    
    batch_dict[4] = remaining_bay_area
    batch_dict[5] = remaining
    batch_dict[6] = matching3
    
    return batch_dict

def determine_partitions(ddf: dd.DataFrame, chunk_size: int):
    n_rows = ddf.map_partitions(len).compute()[0]
    n_parts = n_rows / chunk_size
    
    if int(n_parts) < n_parts:
        n_parts2 = int(n_parts) + 1
    else:
        n_parts2 = int(n_parts)
    
    return n_parts2
    


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
    chunk_size1 = 100_000
    chunk_size2 = 500_000
    
    batch_dict = determine_batches(analysis_date)
    batch_dict2 = {k:v for k, v in batch_dict.items() if k in [6, 1, 5, 0]}
    
    for batch_i, operator_list in batch_dict2.items():
        time0 = datetime.datetime.now()
        
        stop_times = delayed(setup_nearest_neighbor_one_operator)(
            analysis_date, dict_inputs, operator_list) 

        stop_times = dd.from_delayed(stop_times).persist()

        time1 = datetime.datetime.now()
        logger.info(f"persist stop_times dfs: {time1 - time0}")
        
        st_nparts = determine_partitions(stop_times, chunk_size1)
        stop_times = stop_times.repartition(npartitions=st_nparts)
        
        st_dtypes = stop_times.drop(
            columns = ["vp_geometry", "vp_idx"]
        ).dtypes.to_dict()

        nn_dtypes = {
            "nearest_vp_idx": "int",
            "vp_idx_trio": "object",
            "location_timestamp_local_trio": "object",
            "vp_coords_trio": "geometry"
        }
        
        gdf = stop_times.map_partitions(
            nn_result_one_operator,
            analysis_date,
            meta = {
                **st_dtypes,
                **nn_dtypes
            },
            align_dataframes = False
        )

        results = filter_to_valid_nn(gdf).persist()

        results_nparts = determine_partitions(results, chunk_size2)

        results = results.repartition(npartitions=results_nparts)
            
        time2 = datetime.datetime.now()
        logger.info(f"chunks: {time2 - time1}")
        
        writes = [
            delayed(export_operator_results)(
                results.partitions[part_i],
                batch_i,
                part_i,
                analysis_date, 
                EXPORT_FILE
            ) for part_i in range(0, results.npartitions)
        ]
            
        dd.compute(*writes)
        
        del stop_times, results
    
        time3 = datetime.datetime.now()
        logger.info(f"export batch {batch_i}: {time3- time2}")

    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
    
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
