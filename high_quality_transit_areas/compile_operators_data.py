"""
Combine all the downloaded parquets into 1.

Move here from `traffic_ops/prep_data.py`, since 
HQTA needs it to add route info onto stops data.
"""
import datetime as dt
import sys

from loguru import logger

from shared_utils import rt_dates, gtfs_utils
from update_vars import analysis_date, COMPILED_CACHED_VIEWS

logger.add("./logs/compile_operators_data.log")
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")


def grab_selected_date(selected_date: str):
    """
    Create the cached files for stops, trips, stop_times, routes, and route_info
    """
    
    gtfs_utils.all_routelines_or_stops_with_cached(
        dataset = "stops",
        analysis_date = selected_date,
        export_path = COMPILED_CACHED_VIEWS
    )
    
    logger.info("stops compiled and cached")
    
    gtfs_utils.all_trips_or_stoptimes_with_cached(
        dataset = "trips",
        analysis_date = selected_date,
        export_path = COMPILED_CACHED_VIEWS
    )
    
    logger.info("trips compiled and cached")

    gtfs_utils.all_routelines_or_stops_with_cached(
        dataset = "routelines",
        analysis_date = selected_date,
        export_path = COMPILED_CACHED_VIEWS
    )
    
    logger.info("routelines compiled and cached")

    gtfs_utils.all_trips_or_stoptimes_with_cached(
        dataset = "st",
        analysis_date = selected_date,
        export_path = COMPILED_CACHED_VIEWS
    )
    
    logger.info("stop times compiled and cached")

    # stops, trips, stop_times, and routes save directly to GCS already
    
    
if __name__=="__main__":
    logger.info(f"Analysis date: {analysis_date}")    

    start = dt.datetime.now()
    
    grab_selected_date(analysis_date)
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")
