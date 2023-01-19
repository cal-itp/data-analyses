"""
Download all trips for a day.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

import datetime as dt
import pandas as pd
import sys 

from loguru import logger

import operators_for_hqta
from shared_utils import gtfs_utils_v2
from update_vars import analysis_date, COMPILED_CACHED_VIEWS


if __name__=="__main__":
    # Connect to dask distributed client, put here so it only runs 
    # for this script
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/download_data.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
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
        operator_feeds = FEEDS_TO_RUN,
        trip_cols = keep_trip_cols,
        get_df = True,
    ) 
    
    trips.to_parquet(f"{COMPILED_CACHED_VIEWS}{dataset}_{analysis_date}.parquet") 

    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

    #client.close()
