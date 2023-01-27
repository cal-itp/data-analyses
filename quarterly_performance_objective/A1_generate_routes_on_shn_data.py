"""
Script to create PMAC data
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger
from typing import Literal

from bus_service_utils import create_parallel_corridors_v2
from shared_utils import geography_utils
from update_vars import ANALYSIS_DATE, BUS_SERVICE_GCS, COMPILED_CACHED_GCS


def merge_routelines_with_trips(
    selected_date: str, 
    warehouse_version: Literal["v1", "v2"]
) -> gpd.GeoDataFrame: 
    """
    Merge routes and trips to get line geometry.
    """
    shapes = gpd.read_parquet(
        f"{COMPILED_CACHED_GCS}routelines_{selected_date}.parquet")
    trips = pd.read_parquet(
        f"{COMPILED_CACHED_GCS}trips_{selected_date}.parquet")

    if warehouse_version == "v1":
        operator_cols = ["calitp_itp_id"]
    elif warehouse_version == "v2":
        operator_cols = ["feed_key", "name"]
    
    keep_cols = operator_cols + ["route_id", "shape_id", "geometry"]
    
    df = (pd.merge(
            shapes,
            trips,
            on = operator_cols + ["shape_id"],
            how = "inner",
        )[keep_cols]
        .drop_duplicates(subset=operator_cols + ["shape_id"])
        .reset_index(drop=True)
    )
    
    return df
    
    
if __name__ == "__main__":    
    logger.add("./logs/A1_generate_routes_on_shn_data.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}")
    start = datetime.datetime.now()
    
    VERSION = "v1"
    shapes_with_route_info = merge_routelines_with_trips(
        ANALYSIS_DATE, warehouse_version = VERSION)
    
    # 50 ft buffers, get routes that are on SHN
    create_parallel_corridors_v2.make_analysis_data(
        hwy_buffer_feet = 50, 
        transit_routes_df = shapes_with_route_info,
        pct_route_threshold = 0.2, 
        pct_highway_threshold = 0,
        data_path = BUS_SERVICE_GCS, 
        file_name = f"routes_on_shn_{ANALYSIS_DATE}",
        warehouse_version = VERSION
    )  
    
    time1 = datetime.datetime.now()
    logger.info(f"routes on SHN created: {time1 - start}")

    # Grab other routes where at least 35% of route is within 0.5 mile of SHN
    create_parallel_corridors_v2.make_analysis_data(
        hwy_buffer_feet = geography_utils.FEET_PER_MI * 0.5, 
        transit_routes_df = shapes_with_route_info,
        pct_route_threshold = 0.2,
        pct_highway_threshold = 0,
        data_path = BUS_SERVICE_GCS, 
        file_name = f"parallel_or_intersecting_{ANALYSIS_DATE}",
        warehouse_version = VERSION
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"routes within half mile buffer created: {time2 - time1}")
        
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")