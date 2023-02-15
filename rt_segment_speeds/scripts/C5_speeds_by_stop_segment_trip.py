"""
Do linear referencing by stop_segment-trip 
and derive speed.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys
import warnings

from dask import delayed
from loguru import logger
from shapely.errors import ShapelyDeprecationWarning
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

from shared_utils import dask_utils
from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS)

if __name__ == "__main__": 
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("../logs/C5_speeds_by_stop_segment_trip.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    RT_OPERATORS = helpers.operators_with_data(
        f"{SEGMENT_GCS}vp_sjoin/", 'vp_stop_segment_', analysis_date)  
    
    SEGMENT_IDENTIFIER_COLS = ["shape_array_key", "stop_sequence"]
    
    results_linear_ref = []
    
    for rt_dataset_key in RT_OPERATORS:
        time0 = datetime.datetime.now()
        
        # https://docs.dask.org/en/stable/delayed-collections.html
        # Adapt this import to take folder of partitioned parquets
        operator_vp = delayed(
            helpers.import_vehicle_positions)(
            SEGMENT_GCS,
            f"vp_pared_stops_{analysis_date}/",
            file_type = "df",
            filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]],
            partitioned = True
        )        
        
        operator_segments = delayed(helpers.import_segments)(
            SEGMENT_GCS,
            f"stop_segments_{analysis_date}", 
            filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]], 
            columns = ["gtfs_dataset_key",  
                       "geometry"] + SEGMENT_IDENTIFIER_COLS,
        )
         
        time1 = datetime.datetime.now()
        logger.info(f"imported data: {time1 - time0}")
        
        vp_linear_ref = delayed(wrangle_shapes.merge_in_segment_shape)( 
            operator_vp, 
            operator_segments, 
            segment_identifier_cols = SEGMENT_IDENTIFIER_COLS
        )
        
        results_linear_ref.append(vp_linear_ref)

        time2 = datetime.datetime.now()
        logger.info(f"merge in segment shapes and do linear referencing: "
                    f"{time2 - time1}")
    
    
    time3 = datetime.datetime.now()
    logger.info(f"start compute and export of linear referenced results")

    dask_utils.compute_and_export(
        results_linear_ref,
        gcs_folder = SEGMENT_GCS,
        file_name = f"vp_linear_ref_stops_{analysis_date}",
        export_single_parquet = False
    )
    
    time4 = datetime.datetime.now()
    logger.info(f"computed and exported linear ref: {time4 - time3}")
    
    time5 = datetime.datetime.now()
        
    linear_ref_df = delayed(helpers.import_vehicle_positions)(
        SEGMENT_GCS, 
        f"vp_linear_ref_stops_{analysis_date}/",
        file_type = "df", 
        partitioned= True,
    )    

    operator_speeds = delayed(segment_calcs.calculate_speed_by_segment_trip)(
        linear_ref_df, 
        segment_identifier_cols = SEGMENT_IDENTIFIER_COLS,
        timestamp_col = "location_timestamp"
    )
    
    # Save as list to use in compute_and_export
    results_speed = [operator_speeds]
        
    time6 = datetime.datetime.now()
    logger.info(f"calculate speed: {time6 - time5}")
                
    time7 = datetime.datetime.now()
    logger.info(f"start compute and export of speed results")
    
    EXPORTED_SPEED_FILE = f"speed_stops_{analysis_date}"
    
    dask_utils.compute_and_export(
        results_speed, 
        gcs_folder = f"{SEGMENT_GCS}", 
        file_name = EXPORTED_SPEED_FILE,
        export_single_parquet = False
    )
    
    time8 = datetime.datetime.now()
    logger.info(f"exported all speeds: {time8 - time7}")
    
    # Now write out individual parquets for speeds
    speeds_df = dd.read_parquet(
        f"{SEGMENT_GCS}{EXPORTED_SPEED_FILE}/").compute()
    
    for rt_dataset_key in speeds_df.gtfs_dataset_key.unique():
        subset = (speeds_df[speeds_df.gtfs_dataset_key == rt_dataset_key]
                  .reset_index(drop=True)
                 )
        subset.to_parquet(
            f"{SEGMENT_GCS}speeds_by_operator/"
            f"{EXPORTED_SPEED_FILE.split(f'_{analysis_date}')[0]}_"
            f"{rt_dataset_key}_{analysis_date}.parquet"
        )
    
    time9 = datetime.datetime.now()
    logger.info(f"exported operator speed parquets: {time9 - time8}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    #client.close()
        