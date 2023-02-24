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
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed
from loguru import logger

from shared_utils import dask_utils
from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH)


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
    ).drop(columns = "geometry")
    
    in_vp = vp_df.gtfs_dataset_key.compute().tolist()
    in_segments = segment_df.gtfs_dataset_key.compute().tolist()
    RT_OPERATORS = list(set(in_vp) & set(in_segments))
    
    results = []
    
    for rt_dataset_key in RT_OPERATORS:
        
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
        
        for i in vp2[GROUPING_COL].unique().compute():
            SINGLE_SEGMENT_COL = [c for c in SEGMENT_IDENTIFIER_COLS 
                                  if c!= GROUPING_COL]
            
            vp_to_segment = delayed(
                helpers.sjoin_vehicle_positions_to_segments)(
                vp2,
                segments, 
                route_tuple = (GROUPING_COL, i), 
                segment_identifier_cols = SINGLE_SEGMENT_COL
            )

            results.append(vp_to_segment)

    # Compute the list of delayed objects
    dask_utils.compute_and_export(
        results,
        gcs_folder = f"{SEGMENT_GCS}vp_sjoin/",
        file_name = f"{EXPORT_FILE}_{analysis_date}",
        export_single_parquet = False
    )
    
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/A3_sjoin_vp_segments.log"
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
    
    time1 = datetime.datetime.now()
    logger.info(f"attach vp to route segments: {time1 - start}")
    
    vp_stop_seg = sjoin_vp_to_segments(
        analysis_date = analysis_date,
        buffer_size = 50,
        dict_inputs = STOP_SEG_DICT
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"attach vp to stop-to-stop segments: {time2 - time1}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")