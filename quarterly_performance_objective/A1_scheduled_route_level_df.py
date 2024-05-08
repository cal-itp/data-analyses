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

from calitp_data_analysis import geography_utils, utils
from shared_utils import portfolio_utils
from bus_service_utils import create_parallel_corridors
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from update_vars import (BUS_SERVICE_GCS, COMPILED_CACHED_GCS,
                         ANALYSIS_DATE, 
                        )

catalog = intake.open_catalog(
    "../_shared_utils/shared_utils/shared_data_catalog.yml")


def shape_geom_to_route_geom(
    analysis_date: str
) -> gpd.GeoDataFrame: 
    """
    Get longest shape for route-direction and tag
    what category it is for parallel routes.
    """
    longest_shape = gtfs_schedule_wrangling.longest_shape_by_route_direction(
        analysis_date
    ).rename(columns = {"schedule_gtfs_dataset_key": "gtfs_dataset_key"})
    
    operator_cols = ["feed_key", "gtfs_dataset_key"]

    routes = create_parallel_corridors.process_transit_routes(
        longest_shape, operator_cols = operator_cols 
    )
    
    return routes

    
def aggregate_trip_service_to_route_level(
    analysis_date: str,
    route_cols: list
) -> pd.DataFrame:      
    """
    Aggregate trip-level service hours to the route-level.
    """
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = route_cols + ["trip_id", "service_hours"],
        get_pandas = True
    ).rename(columns = {"schedule_gtfs_dataset_key": "gtfs_dataset_key"})
    
    route_service_hours = portfolio_utils.aggregate_by_geography(
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
        keep_geom_type = True
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


if __name__ == "__main__":
    
    logger.add("./logs/quarterly_performance_pipeline.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    route_cols_no_name = ["gtfs_dataset_key", "feed_key", "route_id"]
    route_cols = route_cols_no_name + ["name"]
    
    # Get longest shape by route-direction
    routes = shape_geom_to_route_geom(
        ANALYSIS_DATE)
    
    # Add district    
    route_geom_with_district = add_district(routes, route_cols_no_name)
    
    # Get route-level service
    route_service = aggregate_trip_service_to_route_level(ANALYSIS_DATE, route_cols)
    
    # Merge service hours with route-level geometry and district
    gdf = pd.merge(
        route_geom_with_district,
        route_service,
        on = route_cols_no_name,
        how = "left",
        validate = "1:1"
    )
    
    utils.geoparquet_gcs_export(
        gdf, 
        BUS_SERVICE_GCS,
        f"routes_{ANALYSIS_DATE}.parquet"
    )
    
    end = datetime.datetime.now()
    logger.info(f"route level service: {ANALYSIS_DATE} {end - start}")