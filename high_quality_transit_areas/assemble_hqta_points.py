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

import _utils
from calitp_data_analysis import geography_utils, utils
from segment_speed_utils import helpers
from shared_utils import gtfs_utils_v2
from update_vars import analysis_date, GCS_FILE_PATH, PROJECT_CRS, EXPORT_PATH

catalog = intake.open_catalog("*.yml")

def combine_stops_by_hq_types(crs: str) -> gpd.GeoDataFrame:
    """
    Concatenate combined hqta points across all categories then merge in
    the maximum arrivals for each stop (keep if it shows up in hqta_points) 
    with left merge.
    """  
    rail_ferry_brt = catalog.rail_brt_ferry_stops.read().to_crs(
        crs)
    major_stop_bus = catalog.major_stop_bus.read().to_crs(crs)
    stops_in_corridor = catalog.stops_in_hq_corr.read().to_crs(crs)
    
    trip_count_cols = ["am_max_trips", "pm_max_trips"]

    max_arrivals = pd.read_parquet(
        f"{GCS_FILE_PATH}max_arrivals_by_stop.parquet", 
        columns = ["schedule_gtfs_dataset_key", 
                   "stop_id"] + trip_count_cols
    ).pipe(_utils.primary_rename)
    
    # Combine AM max and PM max into 1 column   
    # if am_max_trips = 4 and pm_max_trips = 5, we'll choose 4.
    max_arrivals = max_arrivals.assign(
        peak_trips = max_arrivals[trip_count_cols].min(axis=1)
    ).drop(columns = trip_count_cols)
    
    hqta_points_combined = pd.concat([
        major_stop_bus,
        stops_in_corridor,
        rail_ferry_brt,
    ], axis=0)
    
    # Merge in max arrivals
    with_stops = pd.merge(
        hqta_points_combined,
        max_arrivals,
        on = ["schedule_gtfs_dataset_key_primary", "stop_id"],
        how = "left"
    ).fillna({"peak_trips": 0}).astype({"peak_trips": "int"})
    
    keep_stop_cols = [
        "schedule_gtfs_dataset_key_primary", "schedule_gtfs_dataset_key_secondary",
        "stop_id", "geometry",
        "hqta_type", "peak_trips", "hqta_details"
    ]
    
    with_stops = with_stops.assign(
        hqta_details = with_stops.apply(_utils.add_hqta_details, axis=1)
    )[keep_stop_cols]
    
    return with_stops


def get_agency_crosswalk(analysis_date: str) -> pd.DataFrame:
    """
    Import crosswalk for changing schedule_gtfs_dataset_key to 
    organization_name/source_record_id
    """
    agency_info = helpers.import_schedule_gtfs_key_organization_crosswalk(
        analysis_date,
        columns = [
            "schedule_gtfs_dataset_key",
            "organization_name", "organization_source_record_id", 
            "base64_url"]
        ).rename(columns = {
        "organization_name": "agency",
        "organization_source_record_id": "org_id"
    })
    
    return agency_info

        
def add_route_agency_info(
    gdf: gpd.GeoDataFrame, 
    analysis_date: str
) -> gpd.GeoDataFrame :
    """
    Make sure route info is filled in for all stops.
    
    Add agency names by merging it in with our crosswalk
    and populate primary and secondary operator information.
    """
    stop_with_route_crosswalk = catalog.stops_info_crosswalk.read()
    
    agency_info = get_agency_crosswalk(analysis_date)
    
    # Make sure all the stops have route_id
    gdf2 = pd.merge(
        gdf,
        stop_with_route_crosswalk[
            ["schedule_gtfs_dataset_key", 
             "stop_id", "route_id"]].drop_duplicates().pipe(_utils.primary_rename),
        on = ["schedule_gtfs_dataset_key_primary", "stop_id"],
        how = "inner"
    )
    
    # Make sure gtfs_dataset_name and organization columns are added
    gdf3 = pd.merge(
        gdf2,
        agency_info.add_suffix("_primary"),
        on = "schedule_gtfs_dataset_key_primary",
        how = "inner"
    ).merge(
        agency_info.add_suffix("_secondary"),
        on = "schedule_gtfs_dataset_key_secondary",
        how = "left" 
        # left bc we don't want to drop rows that have secondary operator
    )
    
    return gdf3


def final_processing(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Final steps for getting dataset ready for Geoportal.
    Subset to columns, drop duplicates, sort for readability,
    always project into WGS84.
    """
    # Clip to CA -- remove ferry or else we're losing it in the clip
    not_ferry = gdf[
        gdf.hqta_type != "major_stop_ferry"
    ].pipe(_utils.clip_to_ca)
    
    is_ferry = gdf[
        gdf.hqta_type == "major_stop_ferry"
    ]
    
    gdf2 = pd.concat(
        [not_ferry, is_ferry], axis=0
    ).reset_index(drop=True)
    
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
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
    
    gdf3 = (
        gdf2[
            (gdf2.schedule_gtfs_dataset_key_primary.isin(public_feeds)) 
        ].reindex(columns = keep_cols)
        .drop_duplicates(
            subset=["agency_primary", "hqta_type", "stop_id", "route_id"])
        .sort_values(["agency_primary", "hqta_type", "stop_id"])
        .reset_index(drop=True)
        .to_crs(geography_utils.WGS84)
    )
    
    return gdf3
   
    
if __name__=="__main__":
        
    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    start = datetime.datetime.now()

    # Combine all the points data and merge in max_arrivals 
    hqta_points_combined = combine_stops_by_hq_types(crs=PROJECT_CRS)    

    # Add in route_id and agency info
    hqta_points_with_info = add_route_agency_info(
        hqta_points_combined, analysis_date)

    gdf = final_processing(hqta_points_with_info)
    
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
    logger.info(
        f"D1_assemble_hqta_points {analysis_date} "
        f"execution time: {end - start}"
    )
    