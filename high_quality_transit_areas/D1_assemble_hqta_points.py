"""
Combine all the points for HQ transit open data portal.

From combine_and_visualize.ipynb

Request: Thank you for this data. It would be useful for us to get the 
HQTA stops as a point data file, not a polygon. Also, if you could 
differentiate between train, bus, BRT, and ferry stop that would be 
immensely helpful. Let me know if it is possible to get the data in this format.
"""
import datetime
import geopandas as gpd
import intake
import os
import pandas as pd
import sys

from loguru import logger

from A1_rail_ferry_brt_stops import clip_to_ca, get_rail_ferry_brt_extract
from calitp_data_analysis import geography_utils, utils
from segment_speed_utils import helpers
from update_vars import analysis_date, GCS_FILE_PATH, PROJECT_CRS

catalog = intake.open_catalog("*.yml")
EXPORT_PATH = f"{GCS_FILE_PATH}export/{analysis_date}/"

def hqta_details(row) -> str:
    """
    Add HQTA details of why nulls are present 
    based on feedback from open data users.
    """
    if row.hqta_type == "major_stop_bus":
        if row.feed_key_primary != row.feed_key_secondary:
            return "intersection_2_bus_routes_different_operators"
        else:
            return "intersection_2_bus_routes_same_operator"
    elif row.hqta_type == "hq_corridor_bus":
        return "stop_along_hq_bus_corridor_single_operator"
    elif row.hqta_type in ["major_stop_ferry", 
                           "major_stop_brt", "major_stop_rail"]:
        # (not sure if ferry, brt, rail, primary/secondary ids are filled in.)
        return row.hqta_type + "_single_operator"
    
    
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
        columns = ["feed_key", "gtfs_dataset_key", "trip_id", "route_id"],
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
        trips[trip_cols + [
            "schedule_gtfs_dataset_key", "route_id"
        ]].drop_duplicates(),
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
        hqta_points_with_route.hqta_type != "major_stop_ferry"
    ].pipe(clip_to_ca)
    
    is_ferry = hqta_points_with_route[
        hqta_points_with_route.hqta_type == "major_stop_ferry"]
    
    ca_hqta_points = pd.concat(
        [not_ferry, is_ferry], axis=0
    ).reset_index(drop=True)
    
    return ca_hqta_points

    
def get_agency_info(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    HQTA analysis uses feed_key to link across schedule tables.
    But, from trips table, we have schedule_gtfs_dataset_key,
    and we can use that to join to our saved crosswalk.
    """
    crosswalk = helpers.import_schedule_gtfs_key_organization_crosswalk(
        date
    ).drop(
        columns = ["itp_id", "caltrans_district", 
                   "schedule_source_record_id"]
    ).rename(columns = {
        "organization_name": "agency",
        "organization_source_record_id": "org_id"
    })[["schedule_gtfs_dataset_key", 
     "agency", "org_id", "base64_url"]]
        
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
        columns = {"feed_key_primary": "feed_key"})[
        ["feed_key", "schedule_gtfs_dataset_key"]
    ].drop_duplicates()
    
    crosswalk = get_agency_info(feeds_df, analysis_date)
    
    agency_info = pd.merge(
        feeds_df,
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    ).drop(columns = "schedule_gtfs_dataset_key")

    # Merge in organization ids for feed_key_primary
    # and feed_key_secondary
    gdf2 = pd.merge(
        gdf,
        agency_info.add_suffix("_primary"),
        on = "feed_key_primary",
        how = "inner"
    ).merge(
        agency_info.add_suffix("_secondary"),
        on = "feed_key_secondary",
        how = "left" 
        # left bc we don't want to drop rows that have secondary operator
    )

    gdf2 = gdf2.assign(
        hqta_details = gdf2.apply(hqta_details, axis=1),
    )
    
    # Additional clarification of hq_corridor_bus, 
    # only for hqta_stops, not hqta_polygons
    gdf2["hqta_details"] = gdf2.apply(
        lambda x: "corridor_frequent_stop" if (
            (x.hqta_type == "hq_corridor_bus") and 
            (x.peak_trips >= 4)
        ) else "corridor_other_stop" if (
            (x.hqta_type == "hq_corridor_bus") and 
            (x.peak_trips < 4) 
        ) else x.hqta_details, axis = 1)
    
    return gdf2  


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
    
    start = datetime.datetime.now()
    
    rail_ferry_brt = get_rail_ferry_brt_extract().to_crs(
        PROJECT_CRS)
    major_stop_bus = catalog.major_stop_bus.read().to_crs(PROJECT_CRS)
    stops_in_corridor = catalog.stops_in_hq_corr.read().to_crs(PROJECT_CRS)
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
        rail_ferry_brt,
    ], axis=0)
    
    hqta_points_combined2 = merge_in_max_arrivals_by_stop(
        hqta_points_combined, max_arrivals_by_stop)
        
    # Add in route_id 
    hqta_points_with_route_info = add_route_info(hqta_points_combined2)
    
    # Add agency names, hqta_details, project back to WGS84
    gdf = add_agency_names_hqta_details(
        hqta_points_with_route_info, analysis_date
    )
    
    cols = [i for i in gdf.columns if "_primary" in i or "_secondary" in i]
    gdf[cols].drop_duplicates().reset_index(drop=True).to_parquet(
        f"{GCS_FILE_PATH}feed_key_org_crosswalk.parquet"
    )
    
    gdf = final_processing(gdf)
    
    # Export to GCS
    # Stash this date's into its own folder
    utils.geoparquet_gcs_export(
        gdf,
        EXPORT_PATH,
        "ca_hq_transit_stops"
    )  
       
    # Overwrite most recent version (other catalog entry constantly changes)
    utils.geoparquet_gcs_export(
        gdf, 
        GCS_FILE_PATH,
        "hqta_points"
   )
    
    end = datetime.datetime.now()
    logger.info(f"D1_assemble_hqta_points {analysis_date} "
                f"execution time: {end - start}")