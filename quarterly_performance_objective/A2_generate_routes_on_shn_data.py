"""
Script to create PMAC data
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from bus_service_utils import create_parallel_corridors
from shared_utils import geography_utils, rt_dates
from update_vars import BUS_SERVICE_GCS

    
if __name__ == "__main__":    
    logger.add("./logs/A1_generate_routes_on_shn_data.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    ANALYSIS_DATE = rt_dates.PMAC["Q4_2022"]
    VERSION = "v1"

    logger.info(f"Analysis date: {ANALYSIS_DATE}   warehouse {VERSION}")
    start = datetime.datetime.now()
    
    transit_routes = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_{ANALYSIS_DATE}_{VERSION}.parquet")
    
    # 50 ft buffers, get routes that are on SHN
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet = 50, 
        transit_routes_df = transit_routes,
        pct_route_threshold = 0.2, 
        pct_highway_threshold = 0,
        data_path = BUS_SERVICE_GCS, 
        file_name = f"routes_on_shn_{ANALYSIS_DATE}_{VERSION}",
        warehouse_version = VERSION
    )  
    
    time1 = datetime.datetime.now()
    logger.info(f"routes on SHN created: {time1 - start}")

    # Grab other routes where at least 35% of route is within 0.5 mile of SHN
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet = geography_utils.FEET_PER_MI * 0.5, 
        transit_routes_df = transit_routes,
        pct_route_threshold = 0.2,
        pct_highway_threshold = 0,
        data_path = BUS_SERVICE_GCS, 
        file_name = f"parallel_or_intersecting_{ANALYSIS_DATE}_{VERSION}",
        warehouse_version = VERSION
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"routes within half mile buffer created: {time2 - time1}")
        
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")