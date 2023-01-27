"""
Categorize routes created from A1_generate_routes_on_shn.py
and save geoparquet.
"""
import datetime
import geopandas as gpd
import intake
import pandas as pd
import sys

from loguru import logger
from typing import Literal

from A1_generate_routes_on_shn_data import merge_routelines_with_trips
from shared_utils import geography_utils, utils
from bus_service_utils import create_parallel_corridors
from update_vars import ANALYSIS_DATE, BUS_SERVICE_GCS, COMPILED_CACHED_GCS

catalog = intake.open_catalog(
    "../_shared_utils/shared_utils/shared_data_catalog.yml")

#---------------------------------------------------------------#
# Data processing - merge trips dfs, tag a route as parallel/on shn/other
#---------------------------------------------------------------#
def merge_trips_with_shape_service_hours(analysis_date: str):
    """
    Merge trips df with trips_with_service_hrs (aggregated to shape_id).
    
    Aggregate this to route_level, since shape_id is confusing
    and leads to double-counting, or merges not going through in full.
    """
    shape_service_hours = (pd.read_parquet(
            f"{BUS_SERVICE_GCS}trips_with_hrs_{analysis_date}.parquet")
            .rename(columns = {"total_service_hours": "service_hours", 
                               "itp_id": "calitp_itp_id"})
        )

    trips = pd.read_parquet(
            f"{COMPILED_CACHED_GCS}trips_{analysis_date}_all.parquet")
    
    trips2 = pd.merge(
        trips,
        shape_service_hours[
            ["calitp_itp_id", "shape_id", "service_hours"]].drop_duplicates(),
        on = ["calitp_itp_id", "shape_id"],
        how = "inner"
    )
    
    trips3 = trips2[["calitp_itp_id", "route_id", 
                     "shape_id", "service_hours"]
                   ].drop_duplicates().reset_index(drop=True)
    
    return trips3
    

def calculate_route_level_service_hours(
    analysis_date: str, 
    warehouse_version: Literal["v1", "v2"]
) -> pd.DataFrame:
    
    route_cols = ["route_id", "route_type"]
    
    if warehouse_version == "v1":
        operator_cols = ["calitp_itp_id"]
        
        trips = merge_trips_with_shape_service_hours(analysis_date)
    
    elif warehouse_version == "v2":
        operator_cols = ["feed_key", "name"]
        
        trips = pd.read_parquet(
            f"{COMPILED_CACHED_GCS}trips_{analysis_date}.parquet")
        
    # Aggregate trips with service hours (at shape_id level) up to route_id
    route_service_hours = geography_utils.aggregate_by_geography(
        trips,
        group_cols = operator_cols + route_cols,
        sum_cols = ["service_hours"]
    ) 
    
    return route_service_hours


def get_unique_routes(df: pd.DataFrame, route_cols: list) -> pd.DataFrame:
    """
    route_cols: list that uniquely identifies an operator-route_id.
    Get it down to unique route for each row. 
    As is, a row is route_id-hwy, the segment of overlap
    """
    # If there are multiple shape_ids for route_id,
    # Keep the one where it's has higher overlap with SHN
    # If it was ever tagged as parallel, let's keep that obs    
    df2 = (df.sort_values(route_cols + ["pct_route", "pct_highway"])
           .drop_duplicates(subset=route_cols, keep="last")
           # keep last because route_cols can 
           # be 2 cols (feed_key, name) or 1 col (calitp_itp_id) and can't control
           # sort order without knowing list length
          )[route_cols]
    
    return df2
    

def mutually_exclusive_groups(df: pd.DataFrame) -> pd.DataFrame:
    # Now, force mutual exclusivity
    def make_mutually_exclusive(row: str) -> str:
        if row.in_on_shn=="both":
            return "on_shn"
        elif (row.in_on_shn=="left_only") and (row.in_intersecting=="both"):
            return "intersects_shn"
        elif (row.in_on_shn=="left_only") and (row.in_intersecting=="left_only"):
            return "other"
    
    df2 = df.assign(
        category = df.apply(lambda x: make_mutually_exclusive(x), axis=1),
        # Flag a unique route, since nunique(route_id) isn't exact, if route_id is 1
        # and many operators share that value
        unique_route = 1
    ).drop(columns = ["in_on_shn", "in_intersecting"])
    
    return df2
    

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

    
def flag_shn_intersecting_routes(
    analysis_date: str,
    route_cols: list
) -> pd.DataFrame:
    """
    Take the trips df (each indiv trip) and aggregated trip_service_hrs df
    (aggregated to shape_id), merge together,
    and flag whether a transit route is on SHN, intersects SHN, or other.
    """
    
    on_shn = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_on_shn_{analysis_date}.parquet")
    
    all_routes = on_shn[route_cols].drop_duplicates().reset_index(drop=True)
    
    on_shn_routes = get_unique_routes(
        on_shn[on_shn.parallel==1], 
        route_cols
    )
    
    intersecting = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}parallel_or_intersecting_{analysis_date}.parquet"
    )
    
    intersecting_routes = get_unique_routes(
        intersecting[intersecting.parallel==1], 
        route_cols
    )
    
    # Merge dummy variables in
    with_on_shn_intersecting = pd.merge(
        all_routes,
        on_shn_routes,
        on = route_cols,
        how = "left",
        validate = "1:1",
        indicator = "in_on_shn"
    ).merge(
        intersecting_routes,
        on = route_cols,
        how = "left",
        validate = "1:1",
        indicator = "in_intersecting"
    )

    # Make sure on_shn, intersects_shn, and other are mutually exclusive categories
    # A route can only fall into 1 of these groups
    with_categories = mutually_exclusive_groups(with_on_shn_intersecting)
    
    return with_categories


if __name__=="__main__":
    logger.add("./logs/A2_categorize_routes.log", retention = "6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")

    logger.info(f"Analysis date: {ANALYSIS_DATE}")
    start = datetime.datetime.now()
    
    VERSION = "v1"
    
    if VERSION == "v1":
        route_cols = ["calitp_itp_id", "route_id"]
    
    elif VERSION == "v2":
        route_cols = ["feed_key", "name", "route_id"]
    
    # (1) Get route's line geom
    shapes = merge_routelines_with_trips(ANALYSIS_DATE, VERSION)
    
    routes = (create_parallel_corridors.process_transit_routes(
            shapes, VERSION
        )[route_cols + ["route_length", "geometry"]]
        .to_crs(geography_utils.CA_StatePlane)
        # make sure units are in feet
    )
    
    routes_with_district = add_district(routes, route_cols)
    
    # (2) Categorize into each group
    routes_flagged = flag_shn_intersecting_routes(ANALYSIS_DATE, route_cols)
    
    time1 = datetime.datetime.now()
    logger.info(f"flag shn or intersecting: {time1 - start}")
    
    # (3) Aggregate service hours up to route-level
    route_service_hours = calculate_route_level_service_hours(
        ANALYSIS_DATE, VERSION)
    
    # (4) Merge all routes (with line geom) with categories and service hours
    df = pd.merge(
        routes_with_district,
        route_service_hours,
        on = route_cols,
        how = "left",
        validate = "1:1"
    ).merge(
        routes_flagged, 
        on = route_cols,
        how = "left",
        validate = "1:1"
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"create route-level df: {time2 - time1}")
    
    # (3) Some cleanup for visualization
    gdf = (df.assign(
        service_hours = df.service_hours.round(2),
        route_length_mi = round(df.route_length / geography_utils.FEET_PER_MI, 2),
        ).drop(columns = ["route_length", "service_hours"])
        .to_crs(geography_utils.WGS84)
    )
    '''
    # Export to GCS (use date suffix because we will want historical comparisons)
    utils.geoparquet_gcs_export(
        gdf, 
        BUS_SERVICE_GCS,
        f"routes_categorized_{ANALYSIS_DATE}"
    )
    
    logger.info("exported dataset to GCS")
    '''
    logger.info("test oct 2022 run, don't save")
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")