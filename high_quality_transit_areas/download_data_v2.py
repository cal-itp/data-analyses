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

from update_vars import analysis_date, CACHED_VIEWS_EXPORT_PATH
from shared_utils import gtfs_utils_v2, geography_utils, rt_utils, utils

@delayed
def get_trips(feed_key: str, analysis_date: str, 
              custom_filtering: dict = None) -> pd.DataFrame:
    """
    Download the trips that ran on selected day.
    """        
    keep_trip_cols = [
            "feed_key", "name", "regional_feed_type", 
            "service_date", "trip_key", "trip_id",
            "route_key", "route_id", "route_type", 
            "route_short_name", "route_long_name", "route_desc",
            "direction_id", 
            "shape_array_key", "shape_id",
            "trip_first_departure_sec", "trip_last_arrival_sec",
            "service_hours"       
    ]
        
    trips = gtfs_utils_v2.get_trips(
        selected_date = analysis_date,
        operator_feeds = [feed_key],
        trip_cols = keep_trip_cols,
        get_df = True,
        custom_filtering = custom_filtering
    )
    
    return trips


@delayed
def get_routelines(feed_key: str, analysis_date: str, 
                   custom_filtering: dict = None) -> gpd.GeoDataFrame:
    """
    Download the route shapes (line geom) from dim_shapes_geo
    associated with shape_ids / trips that ran on selected day.
    """    
    keep_shape_cols = ["feed_key",
                       "shape_id", "shape_array_key",
                       "n_trips", 
                       # n_trips is new column...can help if we want 
                       # to choose between shape_ids
                       # geometry already returned when get_df is True
                      ]
    
    routelines = gtfs_utils_v2.get_shapes(
        selected_date = analysis_date,
        operator_feeds = [feed_key],
        shape_cols = keep_shape_cols,
        get_df = True,
        crs = geography_utils.CA_NAD83Albers,
        custom_filtering = custom_filtering
    )
    
    return routelines


@delayed
def get_stops(feed_key: str, analysis_date: str, 
              custom_filtering: dict = None) -> gpd.GeoDataFrame:
    """
    Download stops for the trips that ran on selected date.
    """       
    # keep all the route_types, use for filtering for rail/brt/ferry later
    route_type_vals = [1, 2, 3, 4, 5, 6, 7, 11, 12, 1_000]
    route_type_cols = [f"route_type_{i}" for i in route_type_vals]
    
    keep_stop_cols = [
        "feed_key",
        "stop_id", "stop_key", "stop_name", 
        "pt_geom",
    ] + route_type_cols 

    
    stops = gtfs_utils_v2.get_stops(
        selected_date = analysis_date,
        operator_feeds = [feed_key],
        stop_cols = keep_stop_cols,
        get_df = True,
        crs = geography_utils.CA_NAD83Albers,
        custom_filtering = custom_filtering
    )
    
    return stops


@delayed
def get_stop_times(feed_key: str, analysis_date: str, 
                   custom_filtering: dict = None) -> pd.DataFrame:
    """
    Download stop times for the trips that ran on selected date.
        
    Write pd.DataFrame in GCS.
    """
    dataset = "st"
    filename = f"{dataset}_{itp_id}_{analysis_date}.parquet"

    full_trips = pd.read_parquet(
        f"{CACHED_VIEWS_EXPORT_PATH}trips_{itp_id}_{analysis_date}.parquet")
    
    stop_times = gtfs_utils_v2.get_stop_times(
        selected_date = analysis_date,
        operator_feeds = [feed_key],
        stop_time_cols = None,
        get_df = True,
        trip_df = full_trips,
        custom_filtering = custom_filtering
    )
    
    if not stop_times.empty:
        stop_times.to_parquet(f"{CACHED_VIEWS_EXPORT_PATH}{filename}")
        logger.info(f"{itp_id}: {dataset} exported to GCS")


def check_route_trips_stops_are_cached(feed_key: str, analysis_date: str):
    """
    Check that routelines, trips, stops parquets are present.
    If not, don't bother downloading stop_times (computationally expensive!).
    
    This way, if an operator is missing files, they are 
    always missing at least 2.
    """
    response1 = rt_utils.check_cached(
        f"routelines_{feed_key}_{analysis_date}.parquet", 
        subfolder="cached_views/")
    response2 = rt_utils.check_cached(
        f"trips_{feed_key}_{analysis_date}.parquet", 
        subfolder="cached_views/")
    response3 = rt_utils.check_cached(
        f"stops_{feed_key}_{analysis_date}.parquet", 
        subfolder="cached_views/")    
    
    all_responses = [response1, response2, response3]
    
    if all(r is not None for r in all_responses):
        return True
    else:
        return False


def compute_delayed(feed_key: str, analysis_date: str, 
                    routelines_df: Delayed, 
                    trips_df: Delayed, 
                    stops_df: Delayed):
    """
    Unpack the result from the dask delayed function. 
    Unpack routelines, trips, and stops first.
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
            f"routelines_{feed_key}_{analysis_date}"
        )
        logger.info(f"{feed_key}: routelines exported to GCS")

        trips.to_parquet(
            f"{CACHED_VIEWS_EXPORT_PATH}trips_{feed_key}_{analysis_date}.parquet")
        logger.info(f"{feed_key}: trips exported to GCS")
        
        utils.geoparquet_gcs_export(
            stops,
            CACHED_VIEWS_EXPORT_PATH,
            f"stops_{feed_key}_{analysis_date}"
        )    
        logger.info(f"{feed_key}: stops exported to GCS")


def compute_delayed_stop_times(
    feed_key: str, 
    analysis_date: str, 
    stop_times_df: Delayed, 
):
    """
    Unpack the stop_times table from the dask delayed function. 
    """
    stop_times = compute(stop_times_df)[0]
    
    if not stop_times.empty:

        stop_times.to_parquet(
            f"{CACHED_VIEWS_EXPORT_PATH}st_{feed_key}_{analysis_date}.parquet")
        logger.info(f"{feed_key}: st exported to GCS")


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
    
    hqta_operators_df = operators_for_hqta.scheduled_operators_for_hqta(analysis_date)
    FEEDS_TO_RUN = hqta_operators_df.feed_key.unique().tolist()    
        
    logger.info(f"# operators to run: {len(FEEDS_TO_RUN)}")
    
    # Store a list where operators have 3 complete files - need stop_times later
    FEEDS_NEED_STOP_TIMES = []
    
    for feed_key in sorted(FEEDS_TO_RUN):
        time0 = dt.datetime.now()
        
        logger.info(f"*********** Download data for: {feed_key} ***********")
        
        # Download routes, trips, stops, stop_times and save as delayed objects
        routelines = get_routelines(feed_key, analysis_date)
        trips = get_trips(feed_key, analysis_date)
        stops = get_stops(feed_key, analysis_date)
        
        # Turn the delayed dask objects into df or gdf and export to GCS
        compute_delayed(feed_key, analysis_date, routelines, trips, stops)
        
        if check_route_trips_stops_are_cached(feed_key, analysis_date):
            IDS_NEED_STOP_TIMES.append(feed_key)
        
        time1 = dt.datetime.now()
        logger.info(f"download files for {itp_id}: {time1-time0}")
    
    
    # Check that routes, trips, stops were downloaded successfully
    # If all 3 are present, then download stop_times. Otherwise, skip stop_times.
    for feed_key in IDS_NEED_STOP_TIMES:
        stop_times = get_stop_times(feed_key, analysis_date)
        
        # Turn the delayed dask objects into df or gdf and export to GCS
        compute_delayed_stop_times(feed_key, analysis_date, stop_times)
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

    client.close()
