"""
Create route-level table combining
service hours and endpoint delay
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from update_vars import ANALYSIS_DATE, BUS_SERVICE_GCS, COMPILED_CACHED_GCS
from shared_utils import geography_utils, utils

logger.add("./logs/A4_route_service_hours_delay.log")
logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")


def calculate_route_level_delays(selected_date: str) -> pd.DataFrame:
    delay_df = gpd.read_parquet(
        f"{COMPILED_CACHED_GCS}endpoint_delays_{selected_date}.parquet")
    
    # Drop duplicates
    delay_df2 = delay_df.drop_duplicates(
        subset=["calitp_itp_id", "route_id", "trip_id", "stop_id"])
    
    route_delay = geography_utils.aggregate_by_geography(
        delay_df2, 
        group_cols = ["calitp_itp_id", "route_id"],
        sum_cols = ["delay_seconds"]
    )
    
    return route_delay


def merge_delay_with_route_categories(route_delay_df: pd.DataFrame, 
                                     routes_categorized: gpd.GeoDataFrame
                                    ) -> gpd.GeoDataFrame:
        
    delay_on_shn_routes = pd.merge(
        routes_categorized.rename(columns = {"itp_id": "calitp_itp_id"}),
        route_delay_df, 
        on = ["calitp_itp_id", "route_id"],
        how = "outer",
        validate = "1:1",
        indicator="merge_delay"
    )
    
    return delay_on_shn_routes


if __name__ == "__main__":
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}")
    start = datetime.datetime.now()
    
    routes_categorized = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_categorized_{ANALYSIS_DATE}.parquet")
    
    # Should I subset to df[df._merge=="both"]?
    # both means that it found a corresponding match in itp_id-route_id 
    # since it's been aggregated up to route_id level (shape_id can mismatch more easily)
    # right_only means found in trips table, but later did not find service hours for it
    logger.info(f"merge results for service hours only: {routes_categorized._merge.value_counts()}")
    
    no_service_hours = routes_categorized[routes_categorized._merge=="right_only"]
    logger.info(f"right only merges for service hours: {no_service_hours.itp_id.value_counts()}")
    
    # Get endpoint delay up to route-level
    route_delay = calculate_route_level_delays(ANALYSIS_DATE)
    
    # Merge the routes that are categorized with route-level delay
    # Keep merge variable to understand why some don't have delay info
    # (it's ok, since not every route will have RT...but why are there some
    # that are in RT but not found in schedule?)
    df = merge_delay_with_route_categories(route_delay, routes_categorized)
    
    logger.info(f"schedule-RT delay outer join merge: {df.merge_delay.value_counts()}")
    
    utils.geoparquet_gcs_export(
        df, 
        BUS_SERVICE_GCS,
        f"routes_categorized_with_delay_{ANALYSIS_DATE}"
    )
    
    logger.info(f"exported to GCS")
    logger.info(f"execution time: {datetime.datetime.now() - start}")