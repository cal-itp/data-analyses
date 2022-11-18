"""
Combine all the downloaded parquets into 1.

Move here from `traffic_ops/prep_data.py`, since 
HQTA needs it to add route info onto stops data.
"""
import datetime as dt
import sys

from loguru import logger
from typing import Literal

from operators_for_hqta import itp_ids_from_json
from shared_utils import rt_dates, gtfs_utils, rt_utils
from update_vars import analysis_date, COMPILED_CACHED_VIEWS

def grab_selected_date(selected_date: str, 
                       dataset: Literal["stops", "trips", "routelines", "st"], 
                       itp_ids: list) -> None:
    """
    Create the cached files for stops, trips, stop_times, routes, and route_info
    """
    compiled_path = rt_utils.check_cached(
        f"{COMPILED_CACHED_VIEWS}{dataset}_{selected_date}.parquet")

    if (dataset in ["stops", "routelines"]) and (not compiled_path):

        gtfs_utils.all_routelines_or_stops_with_cached(
            dataset = dataset,
            analysis_date = selected_date,
            itp_id_list = itp_ids,
            export_path = COMPILED_CACHED_VIEWS
        )
    
        logger.info(f"{dataset} compiled and cached")
    
    if (dataset in ["trips", "st"]) and (not compiled_path):
        gtfs_utils.all_trips_or_stoptimes_with_cached(
            dataset = dataset,
            analysis_date = selected_date,
            itp_id_list = itp_ids,
            export_path = COMPILED_CACHED_VIEWS
        )
    
        logger.info(f"{dataset} compiled and cached")
    
    
if __name__=="__main__":
    
    logger.add("./logs/compile_operators_data.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")

    logger.info(f"Analysis date: {analysis_date}")    

    start = dt.datetime.now()
    ITP_IDS = itp_ids_from_json()
    
    grab_selected_date(analysis_date, "stops", ITP_IDS)
    grab_selected_date(analysis_date, "trips", ITP_IDS)
    grab_selected_date(analysis_date, "routelines", ITP_IDS)
    grab_selected_date(analysis_date, "st", ITP_IDS)
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")
