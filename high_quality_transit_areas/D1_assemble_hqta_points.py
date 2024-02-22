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
from calitp_data_analysis import geography_utils, utils
from shared_utils import schedule_rt_utils
from update_vars import analysis_date, TEMP_GCS, PROJECT_CRS
from segment_speed_utils import helpers

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
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "stop_id", "trip_id"],
        get_pandas = True,
        with_direction = False
    )

    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "trip_id", "route_id"],
        get_pandas = True
    )
    
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
    
    # Clip to CA -- remove ferry or else we're losing it in the clip
    not_ferry = hqta_points_with_route[
        hqta_points_with_route.hqta_type != "major_stop_ferry"]
    is_ferry = hqta_points_with_route[
        hqta_points_with_route.hqta_type == "major_stop_ferry"]
    
    not_ferry_ca = utilities.clip_to_ca(not_ferry)
    ca_hqta_points = pd.concat(
        [not_ferry_ca, is_ferry], axis=0
    ).reset_index(drop=True)
    
    return ca_hqta_points

    
def get_agency_info(df: pd.DataFrame, date: str) -> pd.DataFrame:
    crosswalk = schedule_rt_utils.sample_schedule_feed_key_to_organization_crosswalk(
        df, 
        date,
        quartet_data = "schedule",
        dim_gtfs_dataset_cols = ["key", "base64_url"],
        dim_organization_cols = ["source_record_id", "name"]
    )
        
    return crosswalk
    

def add_agency_names_hqta_details(
    gdf: gpd.GeoDataFrame, 
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Add agency names by merging it in with our crosswalk
    to get the primary feed_key and primary agency name.
    
    Then use a function to add secondary feed_key and secondary agency name 
    and hqta_details column.
    hqta_details makes it clearer for open data portal users why
    some ID / agency name columns show the same info or are missing.
    """
    feeds_df = gdf.rename(
        columns = {"feed_key_primary": "feed_key"})[["feed_key"]].drop_duplicates()
    
    crosswalk = get_agency_info(feeds_df, analysis_date)
    
    NAMES_DICT = dict(zip(
        crosswalk.feed_key, crosswalk.organization_name
    ))
    B64_DICT = dict(zip(
        crosswalk.feed_key, crosswalk.base64_url
    ))
    ORG_DICT = dict(zip(
        crosswalk.feed_key, crosswalk.organization_source_record_id
    ))
    
    gdf = gdf.assign(
        agency_primary = gdf.feed_key_primary.map(NAMES_DICT),
        agency_secondary = gdf.feed_key_secondary.map(NAMES_DICT),
        hqta_details = gdf.apply(utilities.hqta_details, axis=1),
        org_id_primary = gdf.feed_key_primary.map(ORG_DICT),
        org_id_secondary = gdf.feed_key_secondary.map(ORG_DICT),
        base64_url_primary = gdf.feed_key_primary.map(B64_DICT),
        base64_url_secondary = gdf.feed_key_secondary.map(B64_DICT),
    )
    
    # Additional clarification of hq_corridor_bus, 
    # do not put in utilities because hqta_polygons does not share this
    gdf["hqta_details"] = gdf.apply(
        lambda x: "corridor_frequent_stop" if (
            (x.hqta_type == "hq_corridor_bus") and 
            (x.peak_trips >= 4)
        ) else "corridor_other_stop" if (
            (x.hqta_type == "hq_corridor_bus") and 
            (x.peak_trips < 4) 
        ) else x.hqta_details, axis = 1)
        
    return gdf  


def final_processing(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    keep_cols = [
        "agency_primary", 
        "hqta_type", "stop_id", "route_id",
        "hqta_details", "agency_secondary",
        # include these as stable IDs?
        "base64_url_primary", "base64_url_secondary", 
        "org_id_primary", "org_id_secondary",
        "peak_trips",
        "geometry"
    ]
    
    gdf2 = (gdf.reindex(columns = keep_cols)
            .drop_duplicates(
                subset=["agency_primary", "hqta_type", "stop_id", "route_id"])
            .sort_values(["agency_primary", "hqta_type", "stop_id"])
            .reset_index(drop=True)
            .to_crs(geography_utils.WGS84)
    )
    
    return gdf2
    
    
if __name__=="__main__":
        
    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    logger.info(f"D1_assemble_hqta_points Analysis date: {analysis_date}")    
    start = dt.datetime.now()
    
    rail_ferry_brt = rail_ferry_brt_extract.get_rail_ferry_brt_extract().to_crs(
        PROJECT_CRS)
    major_stop_bus = catalog.major_stop_bus.read().to_crs(PROJECT_CRS)
    stops_in_corridor = catalog.stops_in_hq_corr.read().to_crs(PROJECT_CRS)
    max_arrivals_by_stop = pd.read_parquet(
        f"{utilities.GCS_FILE_PATH}max_arrivals_by_stop.parquet", 
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
    logger.info(f"D1_assemble_hqta_points execution time: {end-start}")