"""
Spatial join vehicle positions to segments.

Route segments and stop-to-stop segments.
Use dask.delayed + loop so that vp can only join to the 
relevant segments.
Otherwise, vp will be joined to other segments that share the same road.
"""
import os
os.environ['USE_PYGEOS'] = '0'

import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import gcsfs
import numpy as np
import pandas as pd
import sys

from dask import delayed
from loguru import logger

from shared_utils import dask_utils
from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH)

fs = gcsfs.GCSFileSystem()

def add_grouping_col_to_vp(
    vp_file_name: str,
    analysis_date: str,
    trip_grouping_cols: list
) -> pd.DataFrame:
    """
    Import unique trips present in vehicle positions.
    
    Determine trip_grouping_cols, a list of columns to aggregate trip tables
    up to how segments are cut. 
    Can be ["route_id", "direction_id"] or ["shape_array_key"]
    
    Merge on crosswalk, which gives us feed_key. 
    Be able to link RT to schedule data (gtfs_dataset_key and feed_key present).
    """
    vp_trips = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        vp_file_name,
        columns = ["gtfs_dataset_key", "trip_id"],
        file_type = "df",
    ).drop_duplicates()

    crosswalk = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, ["feed_key", "trip_id"] + trip_grouping_cols)
    
    vp_with_crosswalk = dd.merge(
        vp_trips,
        crosswalk,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    ).compute()
    
    return vp_with_crosswalk


def import_vp_and_merge_crosswalk(
    vp_trips_crosswalk: pd.DataFrame,
    vp_file_name: str,
    grouping_col:  str,
    one_group: str
) -> dg.GeoDataFrame:
    """
    Import vehicle positions, subset to operator.
    
    Determine trip_grouping_cols, a list of columns to aggregate trip tables
    up to how segments are cut. 
    Can be ["route_id", "direction_id"] or ["shape_array_key"]
    
    Merge on crosswalk, which gives us feed_key. 
    Be able to link RT to schedule data (gtfs_dataset_key and feed_key present).
    """
    crosswalk = vp_trips_crosswalk[vp_trips_crosswalk[grouping_col]==one_group]
    RT_OPERATOR = crosswalk.gtfs_dataset_key.iloc[0]
    GRAB_TRIPS = crosswalk.trip_id.unique().tolist()
    
    vp = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        vp_file_name,
        filters = [[
            ("gtfs_dataset_key", "==", RT_OPERATOR),
            ("trip_id", "in", GRAB_TRIPS)]],
        file_type = "gdf",
    )
    
    vp_with_full_info = dd.merge(
        vp,
        crosswalk,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    )
    
    return vp_with_full_info


def import_segments_and_buffer(
    segment_file_name: str,
    buffer_size: int,
    segment_identifier_cols: list,
    grouping_col: str,
) -> gpd.GeoDataFrame:
    """
    Import segments and buffer by specified amount. 
    Only keep columns necessary to find the corresponding vp groupings
    """
    unique_grouping_cols = list(set(segment_identifier_cols + [grouping_col]))
    
    segments = helpers.import_segments(
        SEGMENT_GCS, 
        segment_file_name,
        columns = unique_grouping_cols + [
            "gtfs_dataset_key", "geometry"]  
    )
  
    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments_buff = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    return segments_buff


def sjoin_vp_to_segments(
    vp: dg.GeoDataFrame, 
    segments: gpd.GeoDataFrame
) -> dd.DataFrame:
    """
    Spatial join vp to segments and convert it
    back to tabular, to save space.
    We will pare down vp after this, 
    and will change it back to geospatial after the rows decrease.
    """
    vp_to_seg = dg.sjoin(
        vp,
        segments,
        how = "inner",
        predicate = "within"
    ).drop(columns = "index_right").drop_duplicates().reset_index(drop=True)
    
    
    # Drop geometry and return a df...eventually,
    # can attach point geom back on, after enter/exit points are kept
    # geometry seems to be a big issue in the compute
    vp_to_seg2 = vp_to_seg.assign(
        lon = vp_to_seg.geometry.x,
        lat = vp_to_seg.geometry.y,
    )
    
    ddf = vp_to_seg2.drop(columns = ["geometry"]).repartition(npartitions=1)
    
    return ddf    


def batched_sjoin():

def attach_all_vp_to_segments(
    analysis_date: str,
    buffer_size: int,
    dict_inputs: dict = {}
) -> list[delayed]:
    """
    Spatial join vehicle positions to segments.
    Subset by grouping columns.
    
    Vehicle positions can only join to the relevant segments.
    Use route_dir_identifier or shape_array_key to figure out 
    the relevant segments those vp can be joined to.
    Make use of dask delayed to do this matching of vp to segments.
    """
    INPUT_FILE = dict_inputs["stage1"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    TRIP_GROUPING_COLS = dict_inputs["trip_grouping_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    
    vp_trips_crosswalk = add_grouping_col_to_vp(
        f"{INPUT_FILE}_{analysis_date}",
        analysis_date, 
        TRIP_GROUPING_COLS
    )
    
    segments = delayed(import_segments_and_buffer)(        
        f"{SEGMENT_FILE}_{analysis_date}",
        buffer_size,
        SEGMENT_IDENTIFIER_COLS,
        GROUPING_COL,
    )
    
    in_vp = vp_trips_crosswalk[GROUPING_COL].unique()
    in_segments = segments[GROUPING_COL].unique().compute()
    
    groupings = np.intersect1d(in_vp, in_segments)
    
    logger.info(f"# groups for delayed: {len(groupings)}")
    
    results = []

    for i in groupings:
        start_i = datetime.datetime.now()
        vp_subset = delayed(import_vp_and_merge_crosswalk)(
            vp_trips_crosswalk,
            f"{INPUT_FILE}_{analysis_date}",
            GROUPING_COL,
            i
        )

        segments_subset = (segments[segments[GROUPING_COL]==i]
                        [SEGMENT_IDENTIFIER_COLS + ["geometry"]])

        vp_to_seg = delayed(sjoin_vp_to_segments)(
            vp_subset, segments_subset)
        end_i = datetime.datetime.now()
        print(f"finished {i}: {end_i - start_i}")

        results.append(vp_to_seg)

    return results
    
    '''
    def get_results_for_batch(batch_group: list):
        results = []
        
        for i in batch_group:
            start_i = datetime.datetime.now()
            vp_subset = delayed(import_vp_and_merge_crosswalk)(
                vp_trips_crosswalk,
                f"{INPUT_FILE}_{analysis_date}",
                GROUPING_COL,
                i
            )
        
            segments_subset = (segments[segments[GROUPING_COL]==i]
                            [SEGMENT_IDENTIFIER_COLS + ["geometry"]])
        
            vp_to_seg = delayed(sjoin_vp_to_segments)(
                vp_subset, segments_subset).persist()
            end_i = datetime.datetime.now()
            print(f"finished {i}: {end_i - start_i}")
        
        results.append(vp_to_seg)
        
        return results
    
    chunk_results = []
    
    for chunk in chunked_groups:
        results = get_results_for_batch(chunk)
        
    return chunk_results
    ''' 
    
def compile_partitioned_parquets_for_operators(
    gcs_folder: str,
    file_name: str,
    analysis_date: str, 
):
    """
    Once we've saved out indiv operator partitioned parquets,
    read those in and save as one partitioned parquet
    """
    all_files = fs.ls(f"{gcs_folder}")
    
    # The files to compile need format
    # {file_name}_{batch}_{analysis_date}.
    # do not include {file_name}_{analysis_date} (bc that is concat version)
    files_to_compile = [f for f in all_files 
                        if f"{file_name}_batch" in f and
                        analysis_date in f and 
                        f"{file_name}_{analysis_date}" not in f
                       ]
    
    results = [delayed(pd.read_parquet)(f"gs://{f}") 
                 for f in files_to_compile]
    
    dask_utils.compute_and_export(
        results,
        gcs_folder = gcs_folder,
        file_name = f"{file_name}_{analysis_date}",
        export_single_parquet = False
    )
    
    # Remove these partitioned parquets (folders, set recursive = True)
    for f in files_to_compile:
        fs.rm(f"gs://{f}", recursive=True)
        
        
if __name__ == "__main__":
    
    LOG_FILE = "../logs/sjoin_vp_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    #ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    BUFFER_METERS = 35

    '''
    vp_route_seg = sjoin_vp_to_segments(
        analysis_date = analysis_date,
        buffer_size = 35,
        dict_inputs = ROUTE_SEG_DICT,
    )
    
    compile_partitioned_parquets_for_operators(
        f"{SEGMENT_GCS}vp_sjoin/", 
        ROUTE_SEG_DICT["stage2"], 
        analysis_date
    ) 
    '''
    time1 = datetime.datetime.now()
    #logger.info(f"attach vp to route segments: {time1 - start}")

    vp_stop_seg = attach_all_vp_to_segments(
        analysis_date, 
        BUFFER_METERS, 
        STOP_SEG_DICT
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"attach vp to stop-to-stop segments: {time2 - time1}")
    
    N = 25
    chunked_groups = [vp_stop_seg[i:i + N] 
                      for i in range(0, len(vp_stop_seg), N)]

    
    # Split up the list of delayed objects into chunks
    for i, sub_list in enumerate(chunked_groups):
        dask_utils.compute_and_export(
            sub_list,
            f"{SEGMENT_GCS}vp_sjoin/",
            f'{STOP_SEG_DICT["stage2"]}_batch{i}_{analysis_date}',
            export_single_parquet = False
        ) 
    
    
    time3 = datetime.datetime.now()
    logger.info(f"export batches: {time3 - time2}")
    
    compile_partitioned_parquets_for_operators(
        f"{SEGMENT_GCS}vp_sjoin/", 
        STOP_SEG_DICT["stage2"], 
        analysis_date
    ) 
    
    time4 = datetime.datetime.now()
    logger.info(f"export single partitioned parquet: {time4 - time3}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")