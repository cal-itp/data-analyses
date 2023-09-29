"""
Create route-level table combining
service hours and endpoint delay
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from update_vars import (BUS_SERVICE_GCS, COMPILED_CACHED_GCS, 
                        ANALYSIS_DATE, VERSION
                        )
from shared_utils import portfolio_utils
from calitp_data_analysis import utils

def calculate_route_level_delays(selected_date: str, 
                                 route_cols: list) -> pd.DataFrame:
    """
    Aggregate endpoint_delays to route-level, from trip-level.
    TODO: we should use feed_key for a single day, and at last step bring in
    the more understandable names, but don't drop them beforehand
    """
    delay_df = pd.read_parquet(
        f"{COMPILED_CACHED_GCS}endpoint_delays_{selected_date}.parquet")
    
    # Drop duplicates
    delay_df2 = delay_df.drop_duplicates(
        subset=route_cols + ["trip_id"])
    
    route_delay = portfolio_utils.aggregate_by_geography(
        delay_df2, 
        group_cols = route_cols,
        sum_cols = ["delay_seconds"]
    )
    
    return route_delay


def merge_delay_with_route_categories(
    route_delay_df: pd.DataFrame, 
    routes_categorized: gpd.GeoDataFrame,
    route_cols: list
) -> gpd.GeoDataFrame:
    """
    Merge service hours df with delay df. 
    Service hours contains the route categories: on_shn, intersects_shn, other.
    
    Use an outer join because not every route with service hours (GTFS schedule)
    would have delay hours (GTFS RT). It's also of interest what 
    appears in GTFS-RT, where a GTFS schedule corresponding match wasn't found.
    """
        
    delay_on_shn_routes = pd.merge(
        routes_categorized,
        route_delay_df, 
        on = route_cols,
        how = "outer",
        validate = "1:1",
        indicator="merge_delay"
    )
    
    return delay_on_shn_routes


if __name__ == "__main__":
        
    logger.add("./logs/B2_route_service_hours_delay.log")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}  warehouse {VERSION}")
    start = datetime.datetime.now()
    
    if VERSION == "v1":
        route_cols = ["calitp_itp_id", "route_id"]
        
    elif VERSION == "v2":
        route_cols = ["feed_key", "route_id"]
    
    routes_categorized = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_categorized_{ANALYSIS_DATE}.parquet"
    )
        
    # Get endpoint delay up to route-level
    route_delay = calculate_route_level_delays(ANALYSIS_DATE, route_cols)
    
    # Merge the routes that are categorized with route-level delay
    # Keep merge variable to understand why some don't have delay info
    # (it's ok, since not every route will have RT...but why are there some
    # that are in RT but not found in schedule?)
    df = merge_delay_with_route_categories(route_delay, 
                                           routes_categorized, route_cols)
    
    logger.info(f"schedule-RT delay outer join merge: {df.merge_delay.value_counts()}")
    
    utils.geoparquet_gcs_export(
        df, 
        BUS_SERVICE_GCS,
        f"routes_categorized_with_delay_{ANALYSIS_DATE}"
    )
    
    logger.info(f"exported to GCS")
    logger.info(f"execution time: {datetime.datetime.now() - start}")