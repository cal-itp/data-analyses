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


def merge_vp_with_crosswalk(
    vp_file_name: str,
    analysis_date: str,
    trip_grouping_cols: list
) -> dg.GeoDataFrame:
    
    vp_trips_crosswalk = add_grouping_col_to_vp(
        vp_file_name,
        analysis_date, 
        trip_grouping_cols
    )

    vp = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        vp_file_name,
        file_type = "gdf",
    )
    
    vp_full_info = dd.merge(
        vp,
        vp_trips_crosswalk,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    )
    
    vp2 = vp_full_info.repartition(npartitions=100)
    
    return vp2



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
    unique_grouping_cols = list(
        set(segment_identifier_cols + [grouping_col])
    )
    
    segments = helpers.import_segments(
        SEGMENT_GCS, 
        segment_file_name,
        columns = unique_grouping_cols + ["geometry"]  
    )
  
    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments_buff = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    return segments_buff


def assign_segment_grouping(
    vp: dg.GeoDataFrame,
    segments: gpd.GeoDataFrame,
    segment_identifier_cols: list,
    grouping_col: str
) -> dd.DataFrame:
    
    # Find which groups are present in this partition
    present_groups = vp[grouping_col].unique().tolist()
    
    # Use the groups that are present and search within the buffered segments
    # to grab only rows that are those groups
    # vp for shape1 can only attach to shape1 segments
    segments_pared = (segments[
        segments[grouping_col].isin(present_groups)]
        [segment_identifier_cols + ["geometry"]]
    )
    
    # Do the spatial join, and get rid of any wrong sjoin results 
    # grouping col from vp and segments must match
    # It's easy for vp to attach to lots of segments because major blvds 
    # have multiple shapes running over it
    s1 = dg.sjoin(
        vp,
        segments_pared,
        how = "inner",
        predicate = "within"
    ).drop(columns = "index_right")
    
    s2 = s1[
            s1[f"{grouping_col}_left"]==s1[f"{grouping_col}_right"]
        ][["gtfs_dataset_key", "trip_id", 
           "location_timestamp_local", "stop_sequence"]]
        
    return s2


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
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    dict_inputs = STOP_SEG_DICT

    INPUT_FILE = dict_inputs["stage1"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    TRIP_GROUPING_COLS = dict_inputs["trip_grouping_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    EXPORT_FILE = dict_inputs["stage2"]
    
    BUFFER_METERS = 35
    
    vp = merge_vp_with_crosswalk(
        f"{INPUT_FILE}_{analysis_date}",
        analysis_date,
        TRIP_GROUPING_COLS
    )
    
    segments = import_segments_and_buffer(
        f"{SEGMENT_FILE}_{analysis_date}",
        BUFFER_METERS,
        SEGMENT_IDENTIFIER_COLS,
        GROUPING_COL
    )
    
    ddf = vp.map_partitions(
        assign_segment_grouping,
        segments, 
        SEGMENT_IDENTIFIER_COLS,
        GROUPING_COL,
        align_dataframes = False
    )
    
    print(f"ddf: {ddf.columns}")
    
    time1 = datetime.datetime.now()
    logger.info(f"map partitions: {time1 - start}")
    
    gddf = vp[["gtfs_dataset_key", "shape_array_key",
               "trip_id", "location_timestamp_local", 
               "geometry"]].repartition(npartitions=1)
    
    ddf2 = dd.merge(
        gddf,
        ddf,
        on = ["gtfs_dataset_key", "trip_id", 
              "location_timestamp_local"],
        how = "inner"
    )

    print(f"ddf2 {ddf2.columns}, {type(ddf2)}")

     
    ddf3 = ddf2.assign(
        x = ddf2.geometry.x,
        y = ddf2.geometry.y
    ).drop(columns = "geometry")
    
    ddf3 = ddf3.repartition(npartitions=25)
    
    print(ddf3.columns, type(ddf3))

    ddf3.to_parquet(
        f"{SEGMENT_GCS}vp_usable_sjoin_test_{analysis_date}",
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"export partitioned parquet: {time2 - time1}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")

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
