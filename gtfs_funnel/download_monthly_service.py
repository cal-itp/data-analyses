"""
Download monthly service aggregations.
"""
#import os
#os.environ["CALITP_BQ_MAX_BYTES"] = str(400_000_000_000)

import datetime
import pandas as pd
import siuba  # need this to do type hint in functions
import sys 

from loguru import logger
from siuba import *

from calitp_data_analysis.tables import tbls
from update_vars import GTFS_DATA_DICT
SCHED_GCS = GTFS_DATA_DICT.gcs_paths.SCHED_GCS

def download_one_year(year: int, export_filename: str):
    """
    Download single day for trips.
    """
    start = datetime.datetime.now()
        
    df = (
        tbls.mart_gtfs.fct_monthly_route_service_by_timeofday()
            >> filter(_.year == year)
            >> collect()
        )

    df.to_parquet(
        f"{SCHED_GCS}{export_filename}_{year}.parquet"
    ) 
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    return

    
if __name__=="__main__":
        
    logger.add("./logs/download_data.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    EXPORT = GTFS_DATA_DICT.schedule_tables.monthly_scheduled_service

    for y in [2024]:
        download_one_year(y, EXPORT)