"""
Spatial join vehicle positions to segments.

Route segments and stop-to-stop segments.
Use dask.delayed + loop so that vp can only join to the 
relevant segments.
Otherwise, vp will be joined to other segments that share the same road.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import gcsfs
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed
from loguru import logger

from shared_utils import dask_utils
from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH)

fs = gcsfs.GCSFileSystem()

def import_vp_and_merge_crosswalk( 
    rt_dataset_key: str,
    analysis_date: str,
    trip_grouping_cols:  list 
):
    """
    Import vehicle positions, subset to operator.
    
    Determine trip_grouping_cols, a list of columns to aggregate trip tables
    up to how segments are cut. 
    Can be ["route_id", "direction_id"] or ["shape_array_key"]
    
    Merge on crosswalk, which gives us feed_key. 
    Be able to link RT to schedule data (gtfs_dataset_key and feed_key present).
    """
    vp = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        f"vp_{analysis_date}",
        file_type = "gdf",
        filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]]
    )
    
    crosswalk = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, ["feed_key", "trip_id"] + trip_grouping_cols)
    
    vp_with_crosswalk = dd.merge(
        vp,
        crosswalk,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    )
    
    return vp_with_crosswalk
    

def import_segments_and_buffer(
    segment_file_name: str,
    rt_dataset_key: str,
    analysis_date: str,
    buffer_size: int = 50,
):
    """
    Import segments and subset to operator. 
    Buffer by some specified amount.
    """
    segments = helpers.import_segments(
        SEGMENT_GCS, 
        f"{segment_file_name}_{analysis_date}",
        filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]]
    )
    
    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments_buff = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    return segments_buff


def add_grouping_col_to_vp(
    vp: dg.GeoDataFrame,
    segments: dg.GeoDataFrame,
    trip_grouping_cols: list,
    grouping_col: str,
):
    '''
    If we used a list of columns to aggregate trips up, 
    we need to identify the single grouping column.
    For route segments, since we used 2 columns, create a 3rd column that
    combines those 2. 
    This single grouping column is how we'll subset vp and segments later.
    
    ["route_id", "direction_id"] -> "route_dir_identifier"
    ["shape_array_key"] -> "shape_array_key"
    '''
    group_cols =  ["gtfs_dataset_key"] + trip_grouping_cols
    
    unique_grouping_cols = list(set(group_cols + [grouping_col]))
    segments_new_grouping = segments[
        unique_grouping_cols].drop_duplicates()
    
    # vp_crosswalk contains the trip_ids that are present for this operator
    # Do merge to get the route_dir_identifiers that remain
    vp_with_grouping_col = dd.merge(
        vp,
        segments_new_grouping,
        on = group_cols,
        how = "inner"
    ).reset_index(drop=True).repartition(npartitions=1)
    
    return vp_with_grouping_col
      
    
def loop_over_route_groupings_within_operator(
    operator_vp: dd.DataFrame,
    operator_segments: dg.GeoDataFrame,
    route_groupings: list,
    grouping_col: list,
    segment_identifier_cols: list
): # returns dask delayed object
    """
    Do the loop within an operator. Loop by route_dir_identifier
    or shape_array_key.
    
    Only the vehicle positions for that grouping can sjoin
    to those segments.
    """
    operator_results = []
    
    for i, one_value in enumerate(route_groupings):

        vp_to_segment = delayed(helpers.sjoin_vehicle_positions_to_segments)(
            operator_vp,
            operator_segments, 
            route_tuple = (grouping_col, one_value), 
            segment_identifier_cols = segment_identifier_cols
        )

        # Store each route's results in the operator's results list.
        operator_results.append(vp_to_segment)
    
    return operator_results
    
    
def sjoin_vp_to_segments(
    analysis_date: str,
    buffer_size: int = 50,
    dict_inputs: dict = {}
):
    """
    Spatial join vehicle positions to segments.
    Subset by grouping columns.
    
    Vehicle positions can only join to the relevant segments.
    Use route_dir_identifier or shape_array_key to figure out 
    the relevant segments those vp can be joined to.
    """
    INPUT_FILE = dict_inputs["stage1"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    TRIP_GROUPING_COLS = dict_inputs["trip_grouping_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    EXPORT_FILE = dict_inputs["stage2"]
    
    vp_df = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        f"{INPUT_FILE}_{analysis_date}",
        file_type = "df",
        columns = ["gtfs_dataset_key"],
    )
    
    segment_df = helpers.import_segments(
        SEGMENT_GCS, 
        f"{SEGMENT_FILE}_{analysis_date}",
        columns = ["gtfs_dataset_key", 'geometry']
    ).drop(columns = "geometry").drop_duplicates()
    
    in_vp = vp_df.gtfs_dataset_key.compute().tolist()
    in_segments = segment_df.gtfs_dataset_key.tolist()
    RT_OPERATORS = list(set(in_vp) & set(in_segments))
        
    for rt_dataset_key in sorted(RT_OPERATORS):
        
        start_id = datetime.datetime.now()
        
        vp = delayed(import_vp_and_merge_crosswalk)(
            rt_dataset_key,
            analysis_date,
            TRIP_GROUPING_COLS
        )
        
        segments = delayed(import_segments_and_buffer)(
            SEGMENT_FILE,
            rt_dataset_key,
            analysis_date,
            buffer_size=50
        ).persist()
        
        vp2 = delayed(add_grouping_col_to_vp)(
            vp,
            segments,
            TRIP_GROUPING_COLS,
            GROUPING_COL
        ).persist()
        
        # persist the vehicle positions and segments so we can 
        # easily loop by route_dir_identifier or shape_array_key        
        SINGLE_SEGMENT_COL = [c for c in SEGMENT_IDENTIFIER_COLS 
                      if c!= GROUPING_COL]
        
        all_groups = vp2[GROUPING_COL].unique().compute()
        
        operator_results = loop_over_route_groupings_within_operator(
            vp2,
            segments,
            all_groups, 
            GROUPING_COL,
            SINGLE_SEGMENT_COL
        )
        
        if len(operator_results) > 0:
        
            # Compute the list of delayed objects
            dask_utils.compute_and_export(
                operator_results,
                gcs_folder = f"{SEGMENT_GCS}vp_sjoin/",
                file_name = f"{EXPORT_FILE}_{rt_dataset_key}_{analysis_date}",
                export_single_parquet = False
            )
            
            end_id = datetime.datetime.now()
            print(f"exported: {rt_dataset_key} {end_id - start_id}")
     
    
    
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
    # {file_name}_{rt_dataset_key}_{analysis_date}.
    # do not include {file_name}_{analysis_date} (bc that is concat version)
    files_to_compile = [f for f in all_files 
                        if file_name in f and 
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
    
    ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    vp_route_seg = sjoin_vp_to_segments(
        analysis_date = analysis_date,
        buffer_size = 50,
        dict_inputs = ROUTE_SEG_DICT,
    )
    
    compile_partitioned_parquets_for_operators(
        f"{SEGMENT_GCS}vp_sjoin/", 
        ROUTE_SEG_DICT["stage2"], 
        analysis_date
    ) 
    
    time1 = datetime.datetime.now()
    logger.info(f"attach vp to route segments: {time1 - start}")
    
    vp_stop_seg = sjoin_vp_to_segments(
        analysis_date = analysis_date,
        buffer_size = 50,
        dict_inputs = STOP_SEG_DICT
    )
    
    compile_partitioned_parquets_for_operators(
        f"{SEGMENT_GCS}vp_sjoin/", 
        STOP_SEG_DICT["stage2"], 
        analysis_date
    ) 
    time2 = datetime.datetime.now()
    logger.info(f"attach vp to stop-to-stop segments: {time2 - time1}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")