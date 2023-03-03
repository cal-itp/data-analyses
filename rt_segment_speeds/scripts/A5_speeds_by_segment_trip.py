"""
Do linear referencing by segment-trip 
and derive speed.
"""
import datetime
import pandas as pd
import sys

from dask import delayed
from loguru import logger

from shared_utils import dask_utils
from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)
  

def linear_referencing_and_speed_by_segment(
    analysis_date: str,
    dict_inputs: dict = {}
):
    """
    With just enter / exit points on segments, 
    do the linear referencing to get shape_meters, and then derive speed.
    """
    VP_FILE = dict_inputs["stage3"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]    
    EXPORT_FILE = dict_inputs["stage4"]

    
    # https://docs.dask.org/en/stable/delayed-collections.html
    # Adapt this import to take folder of partitioned parquets
    vp = delayed(
        helpers.import_vehicle_positions)(
        SEGMENT_GCS,
        f"{VP_FILE}_{analysis_date}/",
        file_type = "df",
        partitioned = True
    )

    segments = delayed(helpers.import_segments)(
        SEGMENT_GCS,
        f"{SEGMENT_FILE}_{analysis_date}", 
        columns = ["gtfs_dataset_key",  
                   "geometry"] + SEGMENT_IDENTIFIER_COLS,
    )
    
    vp_linear_ref = delayed(wrangle_shapes.linear_reference_vp_against_segment)( 
        vp, 
        segments, 
        segment_identifier_cols = SEGMENT_IDENTIFIER_COLS
    )
        
    speeds = delayed(segment_calcs.calculate_speed_by_segment_trip)(
        vp_linear_ref, 
        segment_identifier_cols = SEGMENT_IDENTIFIER_COLS,
        timestamp_col = TIMESTAMP_COL
    )    
    
    # Put results into a list to use dask_utils
    results = [speeds]
    dask_utils.compute_and_export(
        results, 
        gcs_folder = SEGMENT_GCS, 
        file_name = f"{EXPORT_FILE}_{analysis_date}",
        export_single_parquet = False
    )


if __name__ == "__main__": 
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    LOG_FILE = "../logs/A5_speeds_by_segment_trip.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")

    linear_referencing_and_speed_by_segment(
        analysis_date, 
        dict_inputs = ROUTE_SEG_DICT
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"speeds for route segments: {time1 - start}")
    
    linear_referencing_and_speed_by_segment(
        analysis_date, 
        dict_inputs = STOP_SEG_DICT
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"speeds for stop segments: {time2 - time1}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {time2-start}")
    
    #client.close()
        