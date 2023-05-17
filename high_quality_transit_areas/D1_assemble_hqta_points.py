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

from loguru import logger

import A3_rail_ferry_brt_extract as rail_ferry_brt_extract
import utilities
from shared_utils import utils, geography_utils, gtfs_utils_v2, portfolio_utils
from update_vars import analysis_date, TEMP_GCS, COMPILED_CACHED_VIEWS

EXPORT_PATH = f"{utilities.GCS_FILE_PATH}export/{analysis_date}/"

catalog = intake.open_catalog("*.yml")
 
def merge_in_max_arrivals_by_stop(
    hqta_points: gpd.GeoDataFrame,
    max_arrivals: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Merge combined hqta points across all categories with
    the maximum arrivals for each stop (keep if it shows up in hqta_points) 
    with left merge.
    """    
    with_stops = pd.merge(
        hqta_points,
        max_arrivals.rename(columns = {"feed_key": "feed_key_primary"}),
        on = ["feed_key_primary", "stop_id"],
        how = "left"
    )
    
    # Combine AM max and PM max into 1 column
    trip_count_cols = ["am_max_trips", "pm_max_trips"]
    
    with_stops2 = with_stops.assign(
        peak_trips = (with_stops[trip_count_cols]
                          .min(axis=1)
                          .fillna(0).astype(int))
    ).drop(columns = trip_count_cols)
    
    return with_stops2

    
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
    cleaned_names = portfolio_utils.standardize_gtfs_dataset_names(cleaned_names)
    
    names_dict = dict(zip(cleaned_names.feed_key, cleaned_names.name))
        
    return names_dict


def attach_base64_url_to_feed_key(
    df: pd.DataFrame, analysis_date: str,
    feed_key_cols: list = ["feed_key_primary", "feed_key_secondary"]
) -> pd.DataFrame:
    """
    We will not use feed_key in the published data, but rather, 
    include base64_url, which should be more stable and acts like itp_id.
    Agency name is already gtfs_dataset_name, but gtfs_dataset_key is not 
    going to be as stable as base64_url.
    """
    feed_to_orgs = gtfs_utils_v2.schedule_daily_feed_to_organization(
        selected_date = analysis_date,
        keep_cols = ["base64_url", "feed_key"],
        get_df = True,
        feed_option = "use_subfeeds"
    )
    
    base64_url_dict = dict(zip(feed_to_orgs.feed_key, 
                               feed_to_orgs.base64_url))

    for c in feed_key_cols:
        # new column will be called base64_url_primary if the column was feed_key_primary
        new_col = f"{c.replace('feed_key', 'base64_url')}"
        df[new_col] = df[c].map(base64_url_dict)
    
    return df
    

def add_agency_names_hqta_details(
    gdf: gpd.GeoDataFrame, analysis_date: str
) -> gpd.GeoDataFrame:
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
    
    # Add base64_urls
    gdf = attach_base64_url_to_feed_key(gdf, analysis_date)
    
    return gdf  


def final_processing(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    keep_cols = [
        "agency_name_primary", 
        "hqta_type", "stop_id", "route_id",
        "hqta_details", "agency_name_secondary",
        # include these as stable IDs?
        "base64_url_primary", "base64_url_secondary", 
        "peak_trips",
        "geometry"
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
    max_arrivals_by_stop = pd.read_parquet(
        f"{GCS_FILE_PATH}max_arrivals_by_stop.parquet", 
        columns = ["feed_key", "stop_id", "am_max_trips", "pm_max_trips"]
    ).rename(columns = {"feed_key": "feed_key_primary"})
    
    # Combine all the points data
    hqta_points_combined = pd.concat([
        major_stop_bus,
        stops_in_corridor,
        # add name at once, rail/ferry/brt is only one with it...
        # but we used it to double check downloads were correct
        rail_ferry_brt.drop(columns = "name_primary"),
    ], axis=0)
    
    hqta_points_combined2 = merge_in_max_arrivals_by_stop(
        hqta_points_combined, max_arrivals_by_stop)
    
    
    time1 = dt.datetime.now()
    logger.info(f"combined points: {time1 - start}")
    
    # Add in route_id 
    hqta_points_with_route_info = add_route_info(hqta_points_combined2)
    
    time2 = dt.datetime.now()
    logger.info(f"add route info: {time2 - time1}")

    # Add agency names, hqta_details, project back to WGS84
    gdf = add_agency_names_hqta_details(
        hqta_points_with_route_info, analysis_date)
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

    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")