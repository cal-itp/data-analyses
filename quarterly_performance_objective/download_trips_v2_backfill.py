"""
Download all trips for a day for v2.

Use this to figure out why v1 and v2 aggregate service hours are so different.s
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

import datetime as dt
import pandas as pd
import sys

from loguru import logger

from shared_utils import gtfs_utils_v2, rt_dates
from update_vars import COMPILED_CACHED_GCS

def scheduled_operators(analysis_date: str):
    """
    This is how HQTA data is downloaded...so do the same here for backfill. 
    """
    all_operators = gtfs_utils_v2.schedule_daily_feed_to_organization(
        selected_date = analysis_date,
        keep_cols = None,
        get_df = True,
        feed_option = "use_subfeeds"
    )

    keep_cols = ["feed_key", "name"]

    operators_to_include = all_operators[keep_cols]
    
    # There shouldn't be any duplicates by name, since we got rid 
    # of precursor feeds. But, just in case, don't allow dup names.
    operators_to_include = (operators_to_include
                            .drop_duplicates(subset="name")
                            .reset_index(drop=True)
                           )
    
    return operators_to_include


if __name__=="__main__":
    logger.add("./logs/download_trips_v2_backfill.log")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    analysis_date = rt_dates.PMAC["Q3_2022"]
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = dt.datetime.now()
    
    operators_df = scheduled_operators(analysis_date)
    
    FEEDS_TO_RUN = sorted(operators_df.feed_key.unique().tolist())    
    
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
    
    trips.to_parquet(
        f"{COMPILED_CACHED_GCS}{dataset}_{analysis_date}_v2.parquet") 

    end = dt.datetime.now()
    logger.info(f"execution time: {end - start}")
