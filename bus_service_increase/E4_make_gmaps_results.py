"""
Compare car `duration_in_traffic` with bus `service_hours`

Run `make_gmaps_requests` to get and cache Google Directions API results.
Pull in cached results and compile.
"""
import geopandas as gpd
import os
import pandas as pd
import sys

from datetime import datetime
from loguru import logger

import shared_utils
from bus_service_utils import utils
from E1_setup_parallel_trips_with_stops import ANALYSIS_DATE, COMPILED_CACHED

logger.add("./logs/make_gmaps_results.log")
logger.add(sys.stderr, 
           format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
           level="INFO")

DATA_PATH = "./gmaps_cache/"
GCS_FILE_PATH = f"{utils.GCS_FILE_PATH}gmaps_cache_{ANALYSIS_DATE}/"

def grab_cached_results(df: pd.DataFrame) -> (list, list):
    result_ids = list(df.identifier_num)

    successful_ids = []
    durations = []

    for i in result_ids:
        try:
            json_dict = utils.open_request_json(i, 
                                                data_path = DATA_PATH, 
                                                gcs_file_path = GCS_FILE_PATH
                                   )
            duration_in_sec = json_dict["legs"][0]["duration_in_traffic"]["value"]
            durations.append(duration_in_sec)
            successful_ids.append(i)
        except:
            logger.info(f"Not found: {i}")
    
    return successful_ids, durations 
            
    
def double_check_lists_match(successful_ids: list, 
                             durations: list) -> pd.DataFrame:            
    # Double check lengths match
    logger.info(f"# results_ids: {len(successful_ids)}")
    logger.info(f"# durations: {len(durations)}")

    if len(successful_ids) == len(durations):
        results_df = pd.DataFrame(
            {'identifier': successful_ids,
             'duration_in_sec': durations,
            })
        
        return results_df
     
        
def compare_travel_time_by_mode(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        car_duration_hours = df.duration_in_sec.divide(60 * 60).round(2),
    )
    
    return df


if __name__ == "__main__":    
    time0 = datetime.now()
    
    df = pd.read_parquet(f"{utils.GCS_FILE_PATH}gmaps_df_{ANALYSIS_DATE}.parquet")
    
    successful_ids, durations = grab_cached_results(df)
    logger.info("Grabbed cached results")
    
    results = double_check_lists_match(successful_ids, durations)

    df2 = pd.merge(
        df, 
        results.rename(columns = {"identifier": "identifier_num"}), 
        on = "identifier_num",
        how = "left", 
        validate = "1:1"
    )
    
    df2 = compare_travel_time_by_mode(df2)
    
    time1 = datetime.now()
    logger.info(f"Compiled results: {time1 - time0}")
    
    # Add in route's line geom
    routelines = gpd.read_parquet(
        f"{COMPILED_CACHED}routelines_{ANALYSIS_DATE}.parquet")
    
    shape_id_cols = ["calitp_itp_id", "shape_id"]
    
    drop_cols = ["origin", "destination", "waypoints"]
    
    gdf = pd.merge(
        routelines[shape_id_cols + ["geometry"]].drop_duplicates(),
        df2.drop(columns = drop_cols),
        on = shape_id_cols,
        how = "inner",
        # many on right because trip_ids can share same shape_id
        validate = "1:m"
    ).to_crs(shared_utils.geography_utils.WGS84)
    
    shared_utils.utils.geoparquet_gcs_export(gdf, 
                                             utils.GCS_FILE_PATH, 
                                             f"gmaps_results_{ANALYSIS_DATE}")
    
    end = datetime.now()
    logger.info(f"Total execution: {end - time0}")
    
