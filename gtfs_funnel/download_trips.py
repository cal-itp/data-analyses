"""
Download all trips for a day.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(400_000_000_000)

import datetime as dt
import pandas as pd
import sys 

from loguru import logger

from shared_utils import gtfs_utils_v2
from update_vars import GTFS_DATA_DICT, COMPILED_CACHED_VIEWS

def get_operators(analysis_date: str):
    """
    Operators to download: favor subfeeds over combined regional feed.
    """
    all_operators = gtfs_utils_v2.schedule_daily_feed_to_gtfs_dataset_name(
                selected_date = analysis_date,
                keep_cols = None,
                get_df = True,
                feed_option = "use_subfeeds"
            ).rename(columns = {"gtfs_dataset_name": "name"})

    keep_cols = ["feed_key", "name"]

    operators_to_include = all_operators[keep_cols]

    # There shouldn't be any duplicates by name, since we got rid 
    # of precursor feeds. But, just in case, don't allow dup names.
    operators_to_include = (operators_to_include
                            .drop_duplicates(subset="name")
                            .reset_index(drop=True)
                           )
    
    return operators_to_include


def download_one_day(analysis_date: str):
    """
    Download single day for trips.
    """
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()

    operators_df = get_operators(analysis_date)

    FEEDS_TO_RUN = sorted(operators_df.feed_key.unique().tolist())    

    logger.info(f"# operators to run: {len(FEEDS_TO_RUN)}")

    dataset = GTFS_DATA_DICT.schedule_downloads.trips

    logger.info(f"*********** Download {dataset} data ***********")

    keep_trip_cols = [
        "feed_key", "gtfs_dataset_key", 
        "name", "regional_feed_type", 
        "service_date", "trip_start_date_pacific",
        "trip_id", 
        "trip_instance_key", # https://github.com/cal-itp/data-infra/pull/2489
        "route_key", "route_id", "route_type", 
        "route_short_name", "route_long_name", "route_desc",
        "direction_id", 
        "shape_array_key", "shape_id",
        "trip_first_departure_datetime_pacific", 
        "trip_last_arrival_datetime_pacific",
        "service_hours",
        "trip_start_date_local_tz", "trip_first_departure_datetime_local_tz",
        "trip_last_arrival_datetime_local_tz"
    ]

    trips = gtfs_utils_v2.get_trips(
        selected_date = analysis_date,
        operator_feeds = FEEDS_TO_RUN,
        trip_cols = keep_trip_cols,
        get_df = True,
    ) 

    trips.to_parquet(
        f"{COMPILED_CACHED_VIEWS}{dataset}_{analysis_date}.parquet") 
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    return

    
if __name__=="__main__":
    
    from update_vars import analysis_date_list
    
    logger.add("./logs/download_data.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    for analysis_date in analysis_date_list:
        download_one_day(analysis_date)