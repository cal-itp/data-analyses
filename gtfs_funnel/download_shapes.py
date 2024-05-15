"""
Download all shapes for a day.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(400_000_000_000)

import datetime as dt
import pandas as pd
import sys 

from loguru import logger

from download_trips import get_operators
from shared_utils import gtfs_utils_v2
from calitp_data_analysis import geography_utils, utils
from update_vars import GTFS_DATA_DICT, COMPILED_CACHED_VIEWS

def download_one_day(analysis_date: str):
    """
    Download single day for shapes.
    """
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
    operators_df = get_operators(analysis_date)
    
    FEEDS_TO_RUN = sorted(operators_df.feed_key.unique().tolist())    
    
    logger.info(f"# operators to run: {len(FEEDS_TO_RUN)}")
    
    # this is how it's named already, keep for continuity
    dataset = GTFS_DATA_DICT.schedule_downloads.shapes
    logger.info(f"*********** Download {dataset} data ***********")

  
    keep_shape_cols = [
        "feed_key", "feed_timezone",
        "service_date", 
        "shape_first_departure_datetime_pacific", 
        "shape_last_arrival_datetime_pacific",
        "shape_id", "shape_array_key",
        "n_trips", 
        # n_trips is new column...can help if we want 
        # to choose between shape_ids
        # geometry already returned when get_df is True
    ]
    
    routelines = gtfs_utils_v2.get_shapes(
        selected_date = analysis_date,
        operator_feeds = FEEDS_TO_RUN,
        shape_cols = keep_shape_cols,
        get_df = True,
        crs = geography_utils.WGS84,
    )
    
    utils.geoparquet_gcs_export(
        routelines, 
        COMPILED_CACHED_VIEWS,
        f"{dataset}_{analysis_date}.parquet"
    )
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

    return


if __name__ == "__main__":
    
    from update_vars import analysis_date_list

    logger.add("./logs/download_data.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    for analysis_date in analysis_date_list:
        download_one_day(analysis_date)