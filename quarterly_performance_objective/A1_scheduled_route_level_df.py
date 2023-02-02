"""
Make a route-level df from trips and shapes.

Must include the route's line geom and service hours and 
district.
"""
import datetime
import geopandas as gpd
import intake
import pandas as pd
import sys

from loguru import logger
from typing import Literal

from shared_utils import geography_utils, utils, rt_dates
from bus_service_utils import create_parallel_corridors
from update_vars import (BUS_SERVICE_GCS, COMPILED_CACHED_GCS,
                         get_filename #,ANALYSIS_DATE
                        )

catalog = intake.open_catalog(
    "../_shared_utils/shared_utils/shared_data_catalog.yml")


def shape_geom_to_route_geom(
    shapes: gpd.GeoDataFrame, 
    trips: pd.DataFrame,
    warehouse_version: Literal["v1", "v2"]
) -> gpd.GeoDataFrame: 
    """
    Merge routes and trips to get line geometry.
    """
    cols_to_keep = ["route_id", "shape_id", "geometry"]
    
    if warehouse_version == "v1":
        operator_cols = ["calitp_itp_id"]
        keep_cols = operator_cols + cols_to_keep
        
    elif warehouse_version == "v2":
        operator_cols = ["feed_key"]
        keep_cols = operator_cols + ["name", "route_type"] + cols_to_keep 
    
    # Merge trips with the shape's line geom, and get it to shape_id level
    df = (pd.merge(
            shapes,
            trips,
            on = operator_cols + ["shape_id"],
            how = "inner",
        )[keep_cols]
        .drop_duplicates(subset=operator_cols + ["shape_id"])
        .reset_index(drop=True)
    )
    
    # Now, get it from shape_id level to route_id level
    routes = (create_parallel_corridors.process_transit_routes(
            df, warehouse_version
        ).to_crs(geography_utils.CA_StatePlane)
        # make sure units are in feet
    )   
    
    return routes

    
def aggregate_trip_service_to_route_level(
    trips: pd.DataFrame,
    route_cols: list) -> pd.DataFrame:      
    
    route_service_hours = geography_utils.aggregate_by_geography(
        trips,
        group_cols = route_cols,
        sum_cols = ["service_hours"]
    
    )
    
    return route_service_hours
    
    
def add_district(route_df: gpd.GeoDataFrame, 
                 route_cols: list) -> gpd.GeoDataFrame:
    """
    Merge in district info (only 1 district per route_id)
    """
    districts = (catalog.caltrans_districts.read()
                 [["DISTRICT", "geometry"]]
                 .rename(columns = {"DISTRICT": "district"})
                ).to_crs(route_df.crs)
    
    # Find overlay and calculate overlay length
    # Keep the district with longest overlay for that route_id
    overlay_results = gpd.overlay(
        route_df[route_cols + ["geometry"]].drop_duplicates(),
        districts,
        how = "intersection",
        keep_geom_type = False
    )
  
    overlay_results = overlay_results.assign(
        overlay_length = overlay_results.geometry.to_crs(
            geography_utils.CA_StatePlane).length
    )
    
    longest_overlay = (overlay_results
                       .sort_values(route_cols + ["overlay_length"])
                       .drop_duplicates(subset=route_cols, keep="last")
                       .reset_index(drop=True)
    )
    
    
    # Only keep 1 district per route
    gdf = pd.merge(
        route_df,
        longest_overlay[route_cols + ["district"]],
        on = route_cols,
        how = "left"
    )
    
    return gdf


def import_data(analysis_date: str, 
                warehouse_version: Literal["v1", "v2"]
               ) -> tuple[pd.DataFrame, gpd.GeoDataFrame]: 
    """
    During v1 to v2 warehouse transition, stage comparisons 
    in both versions for a given date.
    
    But, after v1 warehouse is not updated, just use v2, but
    don't always name it v2.
    """
    trips_file_path = get_filename(
        f"{COMPILED_CACHED_GCS}trips_", analysis_date, warehouse_version)
    routelines_file_path = get_filename(
        f"{COMPILED_CACHED_GCS}routelines_", analysis_date, warehouse_version
    )
    
    trips = pd.read_parquet(trips_file_path)
    routelines = gpd.read_parquet(routelines_file_path)
        
    return trips, routelines


if __name__ == "__main__":
    
    ANALYSIS_DATE = rt_dates.PMAC["Q1_2023"]
    VERSION = "v2"
    
    logger.add("./logs/A1_scheduled_route_level_df.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}   warehouse {VERSION}")
    start = datetime.datetime.now()
    
    if VERSION == "v1":
        route_cols = ["calitp_itp_id", "route_id"]
    
    elif VERSION == "v2":
        route_cols = ["feed_key", "name", "route_id"]
    
    # Import data
    trips, routelines = import_data(ANALYSIS_DATE, VERSION)
    
    # Merge to get shape_level geometry, then pare down to route-level geometry
    route_geom = shape_geom_to_route_geom(
        routelines, trips, VERSION)

    # Add district
    route_geom_with_district = add_district(route_geom, route_cols)
    logger.info("get route-level geometry and district")

    # Get route-level service
    route_service = aggregate_trip_service_to_route_level(trips, route_cols)
    logger.info("get route-level service")
    
    # Merge service hours with route-level geometry and district
    gdf = pd.merge(
        route_geom_with_district,
        route_service,
        on = route_cols,
        how = "left",
        validate = "1:1"
    )
    
    utils.geoparquet_gcs_export(
        gdf, 
        BUS_SERVICE_GCS,
        get_filename("routes_", ANALYSIS_DATE, VERSION)
    )
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")