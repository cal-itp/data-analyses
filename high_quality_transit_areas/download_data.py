"""
Separate the steps in downloading data, caching file in GCS,
from checking whether that file is there.
HQTA will run monthly, more frequent than rt_delay will,
so this is caching it for the first time.

Then run the script to populate hqta_operators.json.

Use the hqta_operators.json later to check whether cache exists.

"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

import datetime as dt
import geopandas as gpd
import glob
import pandas as pd
import siuba
import sys 

from calitp.tables import tbl
from siuba import *
from loguru import logger

import operators_for_hqta

from update_vars import analysis_date, CACHED_VIEWS_EXPORT_PATH
from shared_utils import gtfs_utils, geography_utils, rt_utils, utils

logger.add("./logs/download_data.log")
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")

LOCAL_PATH = "./data/"
date_str = analysis_date

def primary_trip_query(itp_id: int,
                       analysis_date: dt.date):
    """
    Run a trips query. 
    Save as parquet.
    
    Pass this down in other functions where trip table is needed.
    Read in this parquet for trips instead of hitting warehouse again.
    """
    dataset = "trips"
    filename = f"{dataset}_{itp_id}_{date_str}.parquet"

    full_trips = gtfs_utils.get_trips(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        trip_cols = None,
        get_df = True
    )
    
    full_trips.to_parquet(f"{LOCAL_PATH}temp_{filename}")
    logger.info(f"{itp_id}: {dataset} saved locally")


def get_routelines(itp_id: int, 
                   analysis_date: str | dt.date):
    """
    Download the route shapes (line geom) from dim_shapes_geo
    associated with shape_ids / trips that ran on selected day.
    
    Write gpd.GeoDataFrame to GCS.
    """
    dataset = "routelines"
    filename = f'{dataset}_{itp_id}_{date_str}.parquet'
    
    # Read in the full trips table
    full_trips = pd.read_parquet(
        f"{LOCAL_PATH}temp_trips_{itp_id}_{date_str}.parquet")
    
    routelines = gtfs_utils.get_route_shapes(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        get_df = True,
        crs = geography_utils.CA_NAD83Albers,
        trip_df = full_trips
    )
    
    if not routelines.empty:
        utils.geoparquet_gcs_export(routelines, 
                                    CACHED_VIEWS_EXPORT_PATH, 
                                    filename)

        logger.info(f"{itp_id}: {dataset} exported to GCS")
    
    
def get_trips(itp_id: int, analysis_date: str | dt.date):
    """
    Download the trips that ran on selected day.
    TODO: filter for route_types? Or do later?
    
    Write pd.DataFrame to GCS.
    """
    dataset = "trips"
    filename = f"{dataset}_{itp_id}_{date_str}.parquet"
    
    # Read in the full trips table
    full_trips = pd.read_parquet(f"{LOCAL_PATH}temp_{filename}")
    
    keep_trip_cols = [
            "calitp_itp_id", "calitp_url_number", 
            "service_date", "trip_key", "trip_id",
            "route_id", "direction_id", "shape_id",
            "calitp_extracted_at", "calitp_deleted_at"
        ]
    
    # Subset it to columns we want now
    trips = (full_trips[keep_trip_cols]
             .drop_duplicates(subset="trip_id")
             .reset_index(drop=True)
            )

    keep_route_cols = [
        "calitp_itp_id", 
        "route_id", "route_short_name", "route_long_name",
        "route_desc", "route_type"
    ]

    routes = gtfs_utils.get_route_info(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        route_cols = keep_route_cols,
        get_df = True
    )

    trips = (trips
             >> inner_join(_, routes, 
                          on = ["calitp_itp_id", "route_id"])
    )
    if not trips.empty:
        trips.to_parquet(f"{CACHED_VIEWS_EXPORT_PATH}{filename}")
        logger.info(f"{itp_id}: {dataset} exported to GCS")
    
    
def get_stops(itp_id: int, analysis_date: str | dt.date):
    """
    Download stops for the trips that ran on selected date.
    
    Write gpd.GeoDataFrame in GCS.
    """
    dataset = "stops"
    filename = f"{dataset}_{itp_id}_{date_str}.parquet"
        
    keep_stop_cols = [
        "calitp_itp_id", "stop_id", 
        "stop_lat", "stop_lon",
        "stop_name", "stop_key"
    ]
    
    stops = (gtfs_utils.get_stops(
            selected_date = analysis_date,
            itp_id_list = [itp_id],
            stop_cols = keep_stop_cols,
            get_df = True,
            crs = geography_utils.CA_NAD83Albers
        )# should be ok to drop duplicates, but must use stop_id for future joins...
        .drop_duplicates(subset=["calitp_itp_id", "stop_id"])
        .reset_index(drop=True)
    )
    
    if not stops.empty:
        utils.geoparquet_gcs_export(stops, CACHED_VIEWS_EXPORT_PATH, filename)
        logger.info(f"{itp_id}: {dataset} exported to GCS")

        
def get_stop_times(itp_id: int, analysis_date: str | dt.date):
    """
    Download stop times for the trips that ran on selected date.
        
    Write pd.DataFrame in GCS.
    """
    dataset = "st"
    filename = f"{dataset}_{itp_id}_{date_str}.parquet"

    full_trips = pd.read_parquet(f"{LOCAL_PATH}temp_trips_{itp_id}_{date_str}.parquet")
    
    stop_times = gtfs_utils.get_stop_times(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        stop_time_cols = None,
        get_df = True,
        departure_hours = None,
        trip_df = full_trips
    )
    
    if not stop_times.empty:
        stop_times.to_parquet(f"{CACHED_VIEWS_EXPORT_PATH}{filename}")
        logger.info(f"{itp_id}: {dataset} exported to GCS")
    
    
def check_route_trips_stops_are_cached(itp_id: int, date_str: str):
    """
    Check that routelines, trips, stops parquets are present.
    If not, don't bother downloading stop_times (computationally expensive!).
    
    This way, if an operator is missing files, they are 
    always missing at least 2.
    """
    response1 = rt_utils.check_cached(
            f"routelines_{itp_id}_{date_str}.parquet", subfolder="cached_views/")
    response2 = rt_utils.check_cached(
        f"trips_{itp_id}_{date_str}.parquet", subfolder="cached_views/")
    response3 = rt_utils.check_cached(
        f"stops_{itp_id}_{date_str}.parquet", subfolder="cached_views/")    
    
    all_responses = [response1, response2, response3]
    
    if all(r is not None for r in all_responses):
        return True
    else:
        return False

        
if __name__=="__main__":
    
    start = dt.datetime.now()
    
    ALL_IDS = (tbl.gtfs_schedule.agency()
               >> distinct(_.calitp_itp_id)
               >> filter(_.calitp_itp_id != 200, 
                         # Amtrak is always filtered out
                         _.calitp_itp_id != 13)
               >> collect()
              ).calitp_itp_id.tolist()
    
    # ITP IDs already run in the script
    CACHED_IDS = operators_for_hqta.get_list_of_cached_itp_ids(
        date_str, ALL_ITP_IDS = ALL_IDS)
    
    IDS_TO_RUN = list(set(ALL_IDS).difference(set(CACHED_IDS)))
    logger.info(f"# operators to run: {len(IDS_TO_RUN)}")
    
    for itp_id in sorted(IDS_TO_RUN):
        time0 = dt.datetime.now()
        
        logger.info(f"*********** Download data for: {itp_id} ***********")
        
        # Stash a trips table locally to use
        primary_trip_query(itp_id, analysis_date)
        
        # Download routes, trips, stops, stop_times and save in GCS
        get_routelines(itp_id, analysis_date)
        get_trips(itp_id, analysis_date)
        get_stops(itp_id, analysis_date)

        # Check that routes, trips, stops were downloaded successfully
        # If all 3 are present, then download stop_times. Otherwise, skip stop_times.
        if check_route_trips_stops_are_cached(itp_id, date_str) is True:
            get_stop_times(itp_id, analysis_date)

        # Remove full trips file
        trip_file = glob.glob(f"{LOCAL_PATH}temp_trips_{itp_id}_*.parquet")
        for f in trip_file:
            os.remove(f)

        time1 = dt.datetime.now()
        logger.info(f"download files for {itp_id}: {time1-time0}")

    end = dt.datetime.now()
    
    logger.info(f"execution time: {end-start}")
