"""
Script to create PMAC data
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from bus_service_utils import create_parallel_corridors
from calitp_data_analysis import geography_utils
from update_vars import BUS_SERVICE_GCS, ANALYSIS_DATE

    
if __name__ == "__main__":    
    logger.add("./logs/quarterly_performance_pipeline.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
            
    transit_routes = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_{ANALYSIS_DATE}.parquet")

    operator_cols = ["feed_key", "name", "gtfs_dataset_key"]

    # 50 ft buffers, get routes that are on SHN
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet = 50, 
        transit_routes_df = transit_routes,
        pct_route_threshold = 0.2, 
        pct_highway_threshold = 0,
        data_path = BUS_SERVICE_GCS, 
        file_name = f"routes_on_shn_{ANALYSIS_DATE}",
        operator_cols = operator_cols
    )  
    
    # Grab other routes where at least 35% of route is within 0.5 mile of SHN
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet = geography_utils.FEET_PER_MI * 0.5, 
        transit_routes_df = transit_routes,
        pct_route_threshold = 0.2,
        pct_highway_threshold = 0,
        data_path = BUS_SERVICE_GCS, 
        file_name = f"parallel_or_intersecting_{ANALYSIS_DATE}",
        operator_cols = operator_cols
    )
            
    end = datetime.datetime.now()
    logger.info(f"create intermediate dfs: {ANALYSIS_DATE}  {end - start}")