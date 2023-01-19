"""
Separate the steps in downloading data, caching file in GCS,
from checking whether that file is there.
HQTA will run monthly, more frequent than rt_delay will,
so this is caching it for the first time.

Then run the script to populate hqta_operators.json.

Use the hqta_operators.json later to check whether cache exists.

"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(500_000_000_000)

import datetime as dt
import gcsfs
import geopandas as gpd
import pandas as pd
import siuba
import sys 

from calitp.tables import tbls
from dask import delayed, compute
from dask.delayed import Delayed # use this for type hint
from siuba import *
from loguru import logger

import operators_for_hqta

from update_vars import (analysis_date, CACHED_VIEWS_EXPORT_PATH, 
                         COMPILED_CACHED_VIEWS
                        )
from shared_utils import gtfs_utils_v2, geography_utils, rt_utils, utils

@delayed
def get_trips(feed_key: str, analysis_date: str) -> pd.DataFrame:
    """
    Download the trips that ran on selected day.
    """        

        

    
    return trips




def compute_delayed(feed_key: str, analysis_date: str, 
                    routelines_df: Delayed, 
                    trips_df: Delayed, 
                    stops_df: Delayed):
    """
    Unpack the result from the dask delayed function. 
    Unpack routelines, trips, and stops first.
    The delayed object is a tuple, and since we just have 1 object, use 
    [0] to grab it.
    
    Export the pd.DataFrame or gpd.GeoDataFrame to GCS.
    
    https://stackoverflow.com/questions/44602766/unpacking-result-of-delayed-function
    """
    routelines = compute(routelines_df)[0]
    trips = compute(trips_df)[0]
    stops = compute(stops_df)[0]    
    
    if not ((routelines.empty) and (trips.empty) and (stops.empty)):
        
        utils.geoparquet_gcs_export(
            routelines,
            CACHED_VIEWS_EXPORT_PATH,
            f"routelines_{feed_key}_{analysis_date}"
        )
        logger.info(f"{feed_key}: routelines exported to GCS")

        trips.to_parquet(
            f"{CACHED_VIEWS_EXPORT_PATH}trips_{feed_key}_{analysis_date}.parquet")
        logger.info(f"{feed_key}: trips exported to GCS")
        
        utils.geoparquet_gcs_export(
            stops,
            CACHED_VIEWS_EXPORT_PATH,
            f"stops_{feed_key}_{analysis_date}"
        )    
        logger.info(f"{feed_key}: stops exported to GCS")




if __name__=="__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/download_data.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
    fs = gcsfs.GCSFileSystem()

    hqta_operators_df = operators_for_hqta.scheduled_operators_for_hqta(
        analysis_date)
    
    FEEDS_TO_RUN = sorted(hqta_operators_df.feed_key.unique().tolist())    
    
    logger.info(f"# operators to run: {len(FEEDS_TO_RUN)}")
    
    dataset = "trips"
    logger.info(f"*********** Download {dataset} data ***********")

    keep_trip_cols = [
        "feed_key", "name", "regional_feed_type", 
        "service_date", "trip_key", "trip_id",
        "route_key", "route_id", "route_type", 
        "route_short_name", "route_long_name", "route_desc",
        "direction_id", 
        "shape_array_key", "shape_id",
        "trip_first_departure_sec", "trip_last_arrival_sec",
        "service_hours"       
    ]
    
    trips = gtfs_utils_v2.get_trips(
        selected_date = analysis_date,
        operator_feeds = [FEEDS_TO_RUN],
        trip_cols = keep_trip_cols,
        get_df = True,
    ) 
    
    trips.to_parquet(f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")    

    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

    #client.close()
