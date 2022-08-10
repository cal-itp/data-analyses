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

from calitp.tables import tbl
from siuba import *

import operators_for_hqta
from shared_utils import gtfs_utils, rt_utils, geography_utils, utils
from update_vars import analysis_date, date_str, EXPORT_PATH

LOCAL_PATH = "./data/"

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
    full_trips = pd.read_parquet(f"{LOCAL_PATH}temp_{filename}")
    
    routelines = gtfs_utils.get_route_shapes(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        get_df = True,
        crs = geography_utils.CA_NAD83Albers,
        trip_df = full_trip
    )
    
    if not routelines.empty:
        utils.geoparquet_gcs_export(routelines, 
                            EXPORT_PATH, 
                            filename)

        print(f"{itp_id}: {dataset} exported to GCS")
    
    
def get_trips(itp_id: int, analysis_date: str | dt.date, 
              #route_types: list = None
             ):
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
        trips.to_parquet(f"{EXPORT_PATH}{filename}")
        print(f"{itp_id}: {dataset} exported to GCS")
    '''
    # TODO: work this into a later function
    if route_types:
        print(f"filtering to GTFS route types {route_types}")
        trips = trips >> filter(_.route_type.isin(route_types))
    '''
    
    
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
        utils.geoparquet_gcs_export(stops, EXPORT_PATH, filename)
        print(f"{itp_id}: {dataset} exported to GCS")

        
def get_stop_times(itp_id: int, analysis_date: str | dt.date):
    """
    Download stop times for the trips that ran on selected date.
    
    TODO: how to align with gtfs_utils? dd.DataFrame or pd.DataFrame?
    
    Write pd.DataFrame in GCS.
    """
    dataset = "st"
    filename = f"{dataset}_{itp_id}_{date_str}.parquet"

    full_trips = pd.read_parquet(f"{LOCAL_PATH}temp_{filename}")
    
    stop_times = gtfs_utils.get_stop_times(
        selected_date = analysis_date,
        itp_id_list: [itp_id],
        stop_time_cols = None,
        get_df = True,
        departure_hours = None,
        trip_df = full_trips
    )
    
    if not stop_times.empty:
        stop_times.to_parquet(f"{EXPORT_PATH}{filename}")
        print(f"{itp_id}: {dataset} exported to GCS")
    

    

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
    print(f"# operators to run: {len(IDS_TO_RUN)}")
    
    # TODO: rethink cached IDs. For the most part, we know there are some
    # that are erroring, such as ITP ID 21, and we want to ignore those going forward
    # that's caught in completeness check later on, but 
    # at this stage, it'll show there's a bunch of operators we want to keep trying on
    
    for itp_id in sorted(IDS_TO_RUN):
        time0 = dt.datetime.now()
        print(f"*********Download data for: {itp_id}*********")
        
        # Stash a trips table locally to use
        primary_trip_query(itp_id, analysis_date)

        # Download routes, trips, stops, stop_times and save in GCS
        get_routelines(itp_id, analysis_date)
        get_trips(itp_id, analysis_date)
        get_stops(itp_id, analysis_date)
        get_stop_times(itp_id, analysis_date)
        
        # Remove full trips file
        trip_file = glob.glob(f"{LOCAL_PATH}temp_trips_{itp_id}_*.parquet")
        for f in trip_file:
            os.remove(f)
        
        time1 = dt.datetime.now()
        print(f"download files for {itp_id}: {time1 - time0}")

    end = dt.datetime.now()
    print(f"execution time: {end-start}")
    