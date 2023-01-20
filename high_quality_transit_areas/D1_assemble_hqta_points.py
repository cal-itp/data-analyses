"""
Combine all the points for HQ transit open data portal.

From combine_and_visualize.ipynb

Request: Thank you for this data. It would be useful for us to get the 
HQTA stops as a point data file, not a polygon. Also, if you could 
differentiate between train, bus, BRT, and ferry stop that would be 
immensely helpful. Let me know if it is possible to get the data in this format.  
"""
import datetime as dt
import geopandas as gpd
import intake
import os
import pandas as pd
import sys

#from calitp.storage import get_fs, is_cloud # pass GCS credential to dask cluster?
from loguru import logger

import A3_rail_ferry_brt_extract as rail_ferry_brt_extract
import utilities
from shared_utils import utils, geography_utils, portfolio_utils
from update_vars import analysis_date, TEMP_GCS, COMPILED_CACHED_VIEWS

#fs = get_fs()

EXPORT_PATH = f"{utilities.GCS_FILE_PATH}export/{analysis_date}/"

catalog = intake.open_catalog("*.yml")
    
def add_route_info(hqta_points: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Use feed_key-stop_id to add route_id back in, 
    using the trips and stop_times table.
    """    
    stop_times = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}st_{analysis_date}.parquet")
    trips = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    stop_cols = ["feed_key", "stop_id"]
    trip_cols = ["feed_key", "trip_id"]
    
    one_trip = (stop_times[stop_cols + ["trip_id"]]
                .drop_duplicates(subset=stop_cols)
                .reset_index(drop=True)
               )
    
    with_route_info = pd.merge(
        one_trip,
        trips[trip_cols + ["route_id"]].drop_duplicates(),
        on = trip_cols,
        how = "inner",
        validate = "m:1" # one_trip has many stops for that trip
    ).rename(columns = {"feed_key": "feed_key_primary"})

    hqta_points_with_route = pd.merge(
        hqta_points,
        with_route_info,
        on = ["feed_key_primary", "stop_id"],
        how = "inner",
        validate = "m:1"
    ).drop(columns = "trip_id")
    
    # Clip to CA
    ca_hqta_points = utilities.clip_to_ca(hqta_points_with_route)
    
    return ca_hqta_points

    
def get_agency_names() -> dict:
    feed_to_name = (pd.read_parquet(
            f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet", 
            columns = ["feed_key", "name"]
        ).drop_duplicates()
        .reset_index(drop=True)
    )
    
    cleaned_names = portfolio_utils.clean_organization_name(feed_to_name)
    
    names_dict = dict(zip(cleaned_names.feed_key, cleaned_names.name))
        
    return names_dict


def add_agency_names_hqta_details(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add agency names by merging it in with our crosswalk
    to get the primary feed_key and primary agency name.
    
    Then use a function to add secondary feed_key and secondary agency name 
    and hqta_details column.
    hqta_details makes it clearer for open data portal users why
    some ID / agency name columns show the same info or are missing.
    """
    names_dict = get_agency_names()
        
    gdf = gdf.assign(
        agency_name_primary = gdf.feed_key_primary.map(names_dict),
        agency_name_secondary = gdf.feed_key_secondary.map(names_dict),
        hqta_details = gdf.apply(utilities.hqta_details, axis=1),
    )
    
    return gdf  


def final_processing(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    keep_cols = [
        "agency_name_primary", "hqta_type", "stop_id", "route_id",
        "hqta_details", "agency_name_secondary", "geometry"
    ]
    
    gdf2 = (gdf.reindex(columns = keep_cols)
            .drop_duplicates(
                subset=["agency_name_primary", "hqta_type", "stop_id", "route_id"])
            .sort_values(["agency_name_primary", "hqta_type", "stop_id"])
            .reset_index(drop=True)
            .to_crs(geography_utils.WGS84)
    )
    
    return gdf2
    
    
if __name__=="__main__":
        
    logger.add("./logs/D1_assemble_hqta_points.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")    
    start = dt.datetime.now()
    
    rail_ferry_brt = rail_ferry_brt_extract.get_rail_ferry_brt_extract()
    major_stop_bus = catalog.major_stop_bus.read()
    stops_in_corridor = catalog.stops_in_hq_corr.read()
    
    # Combine all the points data
    hqta_points_combined = pd.concat([
        major_stop_bus,
        stops_in_corridor,
        # add name at once, rail/ferry/brt is only one with it...
        # but we used it to double check downloads were correct
        rail_ferry_brt.drop(columns = "name_primary"),
    ], axis=0)
    
    
    time1 = dt.datetime.now()
    logger.info(f"combined points: {time1 - start}")
    
    # Add in route_id 
    hqta_points_with_route_info = add_route_info(hqta_points_combined)
    
    time2 = dt.datetime.now()
    logger.info(f"add route info: {time2 - time1}")

    # Add agency names, hqta_details, project back to WGS84
    gdf = add_agency_names_hqta_details(hqta_points_with_route_info)
    gdf = final_processing(gdf)
    
    time3 = dt.datetime.now()
    logger.info(f"add agency names: {time3 - time2}")
    
    # Export to GCS
    # Stash this date's into its own folder, to convert to geojson, geojsonl later
    utils.geoparquet_gcs_export(
        gdf,
        EXPORT_PATH,
        'ca_hq_transit_stops'
    )  
    
    logger.info("export as geoparquet in date folder")
   
    # Overwrite most recent version (other catalog entry constantly changes)
    utils.geoparquet_gcs_export(
        gdf, 
        utilities.GCS_FILE_PATH,
        'hqta_points'
   )
    
    logger.info("export as geoparquet")
        
    # Add geojson / geojsonl exports
    utils.geojson_gcs_export(
        gdf, 
        EXPORT_PATH,
        'ca_hq_transit_stops', 
        geojson_type = "geojson"
    )
    
    logger.info("export as geojson")
    
    utils.geojson_gcs_export(
        gdf, 
        EXPORT_PATH,
        'ca_hq_transit_stops', 
        geojson_type = "geojsonl"
    )
    
    logger.info("export as geojsonl")

    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")