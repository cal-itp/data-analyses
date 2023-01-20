"""
Download all stop_times for a day.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(500_000_000_000)

import datetime as dt
import geopandas as gpd
import pandas as pd
import sys 

from loguru import logger

from shared_utils import gtfs_utils_v2
from update_vars import analysis_date, COMPILED_CACHED_VIEWS


def feeds_across_trips_shapes_stops(analysis_date: str) -> list:
    """
    Return only the feeds that are in common 
    across the trips, shapes, and stops tables.
    
    Use this to subset the trips table down when 
    inputting that for stop_times.
    
    Bug: shapes doesn't have Metrolink feeds, because shape_array_keys are None.
    """
    keep_cols = ["feed_key"]
    
    trip_feeds = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet", 
        columns = keep_cols
    ).drop_duplicates().feed_key.unique().tolist()
    
    stop_feeds = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet",
        columns = keep_cols
    ).drop_duplicates().feed_key.unique().tolist()
    
    shape_feeds = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet",
        columns = keep_cols
    ).drop_duplicates().feed_key.unique().tolist()
    
    feeds_in_common = list(set(trip_feeds) & set(shape_feeds) & set(stop_feeds))
    
    return feeds_in_common


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
    
    full_trips = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    FEEDS_TO_RUN = full_trips.feed_key.unique().tolist()
    #FEEDS_TO_RUN = feeds_across_trips_shapes_stops(analysis_date)

    logger.info(f"# operators to run: {len(FEEDS_TO_RUN)}")
    
    # There may be some feeds missing with complete info
    # subset it now -- should we drop Metrolink knowingly?
    #trips_on_day = full_trips[
    #    full_trips.feed_key.isin(FEEDS_TO_RUN)
    #].reset_index(drop=True)
    
    # st already used, keep for continuity
    dataset = "st"
    logger.info(f"*********** Download {dataset} data ***********")

    keep_stop_time_cols = [
        "feed_key",   
        "trip_id", 
        "stop_id", "stop_sequence", "timepoint",
        "arrival_sec", "departure_sec",
    ]
    
    stop_times = gtfs_utils_v2.get_stop_times(
        selected_date = analysis_date,
        operator_feeds = FEEDS_TO_RUN,
        stop_time_cols = keep_stop_time_cols,
        get_df = True,
        trip_df = full_trips
    ) 
    
    stop_times.to_parquet(
        f"{COMPILED_CACHED_VIEWS}{dataset}_{analysis_date}.parquet") 

    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

    #client.close()