"""
Just run stop_times download.
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

from update_vars import analysis_date, CACHED_VIEWS_EXPORT_PATH
from shared_utils import gtfs_utils_v2, geography_utils, rt_utils, utils


@delayed
def get_stop_times(feed_key: str, analysis_date: str) -> pd.DataFrame:
    """
    Download stop times for the trips that ran on selected date.
        
    Write pd.DataFrame in GCS.
    """
    dataset = "st"
    filename = f"{dataset}_{feed_key}_{analysis_date}.parquet"

    full_trips = pd.read_parquet(
        f"{CACHED_VIEWS_EXPORT_PATH}trips_{feed_key}_{analysis_date}.parquet")
    
    stop_times = gtfs_utils_v2.get_stop_times(
        selected_date = analysis_date,
        operator_feeds = [feed_key],
        stop_time_cols = None,
        get_df = True,
        trip_df = full_trips,
    )
    
    if not stop_times.empty:
        stop_times.to_parquet(f"{CACHED_VIEWS_EXPORT_PATH}{filename}")
        logger.info(f"{feed_key}: {dataset} exported to GCS")


def check_route_trips_stops_are_cached(feed_key: str, analysis_date: str):
    """
    Check that routelines, trips, stops parquets are present.
    If not, don't bother downloading stop_times (computationally expensive!).
    
    This way, if an operator is missing files, they are 
    always missing at least 2.
    """
    response1 = rt_utils.check_cached(
        f"routelines_{feed_key}_{analysis_date}.parquet", 
        subfolder="cached_views/")
    response2 = rt_utils.check_cached(
        f"trips_{feed_key}_{analysis_date}.parquet", 
        subfolder="cached_views/")
    response3 = rt_utils.check_cached(
        f"stops_{feed_key}_{analysis_date}.parquet", 
        subfolder="cached_views/")    
    
    all_responses = [response1, response2, response3]
    
    if all(r is not None for r in all_responses):
        return True
    else:
        return False

    
def compute_delayed_stop_times(
    feed_key: str, 
    analysis_date: str, 
    stop_times_df: Delayed, 
):
    """
    Unpack the stop_times table from the dask delayed function. 
    """
    df = compute(stop_times_df)[0]
    
    if not stop_times.empty:

        df.to_parquet(
            f"{CACHED_VIEWS_EXPORT_PATH}st_{feed_key}_{analysis_date}.parquet")
        logger.info(f"{feed_key}: st exported to GCS")
        
        
if __name__ == "__main__":
    
    hqta_operators_df = operators_for_hqta.scheduled_operators_for_hqta(
        analysis_date)
    
    FEEDS_TO_RUN = sorted(hqta_operators_df.feed_key.unique().tolist())    

    FEEDS_NEED_STOP_TIMES = []

    # Store a list where operators have 3 complete files - need stop_times later
    for feed_key in FEEDS_TO_RUN:
        if check_route_trips_stops_are_cached(feed_key, analysis_date):
            FEEDS_NEED_STOP_TIMES.append(feed_key)
            
    
    # Check that routes, trips, stops were downloaded successfully
    # If all 3 are present, then download stop_times. Otherwise, skip stop_times.
    for feed_key in FEEDS_NEED_STOP_TIMES:
        time0 = dt.datetime.now()
        logger.info(f"*********** Download stop_times for: {feed_key} ***********")

        stop_times = get_stop_times(feed_key, analysis_date)
        
        # Turn the delayed dask objects into df or gdf and export to GCS
        compute_delayed_stop_times(feed_key, analysis_date, stop_times)
        
        time1 = dt.datetime.now()
        logger.info(f"download files for {feed_key}: {time1-time0}")