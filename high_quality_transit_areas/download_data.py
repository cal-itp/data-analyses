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
import gcsfs
import geopandas as gpd
import pandas as pd
import siuba
import sys 

from calitp.tables import tbls
from dask import delayed, compute
from dask.delayed import Delayed # use this for type hint
from siuba import *
from loguru import logger

import operators_for_hqta

from update_vars import analysis_date, TEMP_GCS #CACHED_VIEWS_EXPORT_PATH, TEMP_GCS
from shared_utils import gtfs_utils, geography_utils, rt_utils, utils

CACHED_VIEWS_EXPORT_PATH = "gs://calitp-analytics-data/data-analyses/dask_test/"

def primary_trip_query(itp_id: int, analysis_date: str, 
                       additional_filters: dict = None):
    """
    Run a trips query. 
    Save as parquet.
    
    Pass this down in other functions where trip table is needed.
    Read in this parquet for trips instead of hitting warehouse again.
    """
    dataset = "trips"
    filename = f"{dataset}_{itp_id}_{analysis_date}.parquet"

    full_trips = gtfs_utils.get_trips(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        trip_cols = None,
        get_df = True,
        custom_filtering = additional_filters
    )
    
    full_trips.to_parquet(f"{TEMP_GCS}temp_{filename}")
    logger.info(f"{itp_id}: {dataset} saved in temp GCS")


def get_routelines(itp_id: int, analysis_date: str, 
                   additional_filters: dict = None) -> Delayed:
    """
    Download the route shapes (line geom) from dim_shapes_geo
    associated with shape_ids / trips that ran on selected day.
    """    
    # Read in the full trips table
    full_trips = pd.read_parquet(
        f"{TEMP_GCS}temp_trips_{itp_id}_{analysis_date}.parquet")
    
    routelines = gtfs_utils.get_route_shapes(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        get_df = True,
        crs = geography_utils.CA_NAD83Albers,
        trip_df = full_trips, 
        custom_filtering = additional_filters
    )[["calitp_itp_id", "calitp_url_number", "shape_id", "geometry"]]
    
    return routelines
    
    
def get_trips(itp_id: int, analysis_date: str, 
              additional_filters: dict) -> Delayed:
    """
    Download the trips that ran on selected day.
    """
    dataset = "trips"
    filename = f"{dataset}_{itp_id}_{analysis_date}.parquet"
    
    # Read in the full trips table
    full_trips = pd.read_parquet(f"{TEMP_GCS}temp_{filename}")
    
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
        get_df = True, 
        custom_filtering = additional_filters
    )

    trips = (trips
             >> inner_join(_, routes, 
                          on = ["calitp_itp_id", "route_id"])
             >> gtfs_utils.filter_custom_col(additional_filters)
    )
    
    return trips
    
    
def get_stops(itp_id: int, analysis_date: str, 
              additional_filters: dict) -> Delayed:
    """
    Download stops for the trips that ran on selected date.
    """        
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
            crs = geography_utils.CA_NAD83Albers,
            custom_filtering = additional_filters
        )# should be ok to drop duplicates, but must use stop_id for future joins...
        .drop_duplicates(subset=["calitp_itp_id", "stop_id"])
        .reset_index(drop=True)
    )
    
    return stops
    
        
def get_stop_times(itp_id: int, analysis_date: str, 
                   additional_filters: dict = None):
    """
    Download stop times for the trips that ran on selected date.
        
    Write pd.DataFrame in GCS.
    """
    dataset = "st"
    filename = f"{dataset}_{itp_id}_{analysis_date}.parquet"

    full_trips = pd.read_parquet(
        f"{TEMP_GCS}temp_trips_{itp_id}_{analysis_date}.parquet")
    
    stop_times = gtfs_utils.get_stop_times(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        stop_time_cols = None,
        get_df = True,
        departure_hours = None,
        trip_df = full_trips,
        custom_filtering = additional_filters
    )
    
    if not stop_times.empty:
        stop_times.to_parquet(f"{CACHED_VIEWS_EXPORT_PATH}{filename}")
        logger.info(f"{itp_id}: {dataset} exported to GCS")
    
    
def check_route_trips_stops_are_cached(itp_id: int, analysis_date: str):
    """
    Check that routelines, trips, stops parquets are present.
    If not, don't bother downloading stop_times (computationally expensive!).
    
    This way, if an operator is missing files, they are 
    always missing at least 2.
    """
    response1 = rt_utils.check_cached(
        f"routelines_{itp_id}_{analysis_date}.parquet", 
        subfolder="cached_views/")
    response2 = rt_utils.check_cached(
        f"trips_{itp_id}_{analysis_date}.parquet", 
        subfolder="cached_views/")
    response3 = rt_utils.check_cached(
        f"stops_{itp_id}_{analysis_date}.parquet", 
        subfolder="cached_views/")    
    
    all_responses = [response1, response2, response3]
    
    if all(r is not None for r in all_responses):
        return True
    else:
        return False

    
def compute_delayed(itp_id: int, analysis_date: str, 
                    routelines_df: Delayed, 
                    trips_df: Delayed, 
                    stops_df: Delayed):
    """
    Unpack the result from the dask delayed function. 
    The delayed object is a tuple, and since we just have 1 object, use 
    [0] to grab it.
    
    Export the pd.DataFrame or gpd.GeoDataFrame to GCS.
    
    https://stackoverflow.com/questions/44602766/unpacking-result-of-delayed-function
    """
    routelines = compute(routelines_df)[0]
    trips = compute(trips_df)[0]
    stops = compute(stops_df)[0]    
    
    if not ((routelines.empty) and (trips.empty) and (stops.empty)):
        
        utils.geoparquet_gcs_export(
            routelines,
            CACHED_VIEWS_EXPORT_PATH,
            f"routelines_{itp_id}_{analysis_date}"
        )
        logger.info(f"{itp_id}: routelines exported to GCS")

        trips.to_parquet(
            f"{CACHED_VIEWS_EXPORT_PATH}trips_{itp_id}_{analysis_date}.parquet")
        logger.info(f"{itp_id}: trips exported to GCS")
        
        utils.geoparquet_gcs_export(
            stops,
            CACHED_VIEWS_EXPORT_PATH,
            f"stops_{itp_id}_{analysis_date}"
        )    
        logger.info(f"{itp_id}: stops exported to GCS")

        
def remove_temp_trip_files(gcs_folder: str = TEMP_GCS):
    """
    Remove the temporary full trips file saved in GCS.
    """
    fs_list = fs.ls(gcs_folder)

    # Remove full trips file
    temp_trip_files = [i for i in fs_list if 'temp_' in i]
    
    for f in temp_trip_files:
        fs.rm(f)


if __name__=="__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    from dask.distributed import Client
    
    client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/download_data.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
    fs = gcsfs.GCSFileSystem()
    
    ALL_IDS = (tbls.gtfs_schedule.agency()
               >> distinct(_.calitp_itp_id)
               >> filter(_.calitp_itp_id != 200, 
                         # Amtrak is always filtered out
                         _.calitp_itp_id != 13)
               >> collect()
              ).calitp_itp_id.tolist()
    
    
    # ITP IDs already run in the script
    CACHED_IDS = operators_for_hqta.get_list_of_cached_itp_ids(
        analysis_date, all_itp_ids = ALL_IDS)
    
    IDS_TO_RUN = list(set(ALL_IDS).difference(set(CACHED_IDS)))
    
    logger.info(f"# operators to run: {len(IDS_TO_RUN)}")
        
    for itp_id in sorted(IDS_TO_RUN):
        time0 = dt.datetime.now()
        
        logger.info(f"*********** Download data for: {itp_id} ***********")
        
        # Stash a trips table locally to use
        primary_trip_query(itp_id, analysis_date)

        # Download routes, trips, stops, stop_times and save as delayed objects
        routelines = delayed(get_routelines)(itp_id, analysis_date)
        trips = delayed(get_trips)(itp_id, analysis_date)
        stops = delayed(get_stops)(itp_id, analysis_date)
        
        # Turn the delayed dask objects into df or gdf and export to GCS
        compute_delayed(itp_id, analysis_date, routelines, trips, stops)

        # Check that routes, trips, stops were downloaded successfully
        # If all 3 are present, then download stop_times. Otherwise, skip stop_times.
        if check_route_trips_stops_are_cached(itp_id, analysis_date):
            get_stop_times(itp_id, analysis_date)

        time1 = dt.datetime.now()
        logger.info(f"download files for {itp_id}: {time1-time0}")

    # Delete temp trip files
    # TODO: in v2, if entirety of table is stored in dbt, we won't have to cache
    # and pull from caches across other tables
    remove_temp_trip_files(TEMP_GCS)
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

    client.close()