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
                         ANALYSIS_DATE, VERSION
                        )

catalog = intake.open_catalog(
    "../_shared_utils/shared_utils/shared_data_catalog.yml")


def shape_geom_to_route_geom(
    analysis_date: str
) -> gpd.GeoDataFrame: 
    """
    Merge routes and trips to get line geometry.
    """
    cols_to_keep = ["route_id", "shape_id", "geometry"]
    operator_cols = ["feed_key", "name"]
    
    df = gtfs_schedule_wrangling.get_trips_with_geom(
        analysis_date,
        trip_cols = ["feed_key", "name", 
                     "trip_id",
                     "shape_id","shape_array_key", 
                     "route_id", "route_type"
                    ],
        exclude_me = ["Amtrak Schedule", "*Flex"],
        crs = geography_utils.CA_StatePlane
        # make sure units are in feet
    )
       
    # Merge trips with the shape's line geom, and get it to shape_id level
    df = (df[operator_cols + cols_to_keep + ["route_type"]]
        .drop_duplicates(subset=operator_cols + ["shape_id"])
        .reset_index(drop=True)
    )
    
    # Now, get it from shape_id level to route_id level
    routes = create_parallel_corridors.process_transit_routes(
        df, "v2" 
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
    )
    
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
    
    logger.add("./logs/A1_scheduled_route_level_df.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}   warehouse {VERSION}")
    start = datetime.datetime.now()
    
    route_cols = ["feed_key", "name", "route_id"]
    
    # Merge to get shape_level geometry, then pare down to route-level geometry
    route_geom = shape_geom_to_route_geom(
        ANALYSIS_DATE)

    # Add district
    route_geom_with_district = add_district(route_geom, route_cols)
    logger.info("get route-level geometry and district")

    # Get route-level service
    route_service = aggregate_trip_service_to_route_level(ANALYSIS_DATE, route_cols)
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
        f"routes_{ANALYSIS_DATE}.parquet"
    )
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")