"""
Check GCS bucket: data-analyses/rt_delay/cached_views/
to see if there are already routelines, stops, trips, stop_times 
parquets saved.

If not, run a fresh query using gtfs_utils, which is more generalized,
and can help clean up some lines of code.

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
import pandas as pd

from calitp.tables import tbl
from siuba import *

from B1_bus_corridors import TEST_GCS_FILE_PATH
from shared_utils import gtfs_utils, rt_utils, geography_utils, utils
 
analysis_date = dt.date(2022, 7, 13)
date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)
EXPORT_PATH = f"{TEST_GCS_FILE_PATH}cached_views/"


def keys_for_operator_day(itp_id: int, 
                          analysis_date: dt.date
                         ) -> siuba.sql.verbs.LazyTbl:
    """
    Use trip table to grab all the keys we need.
    For that selected date, ITP ID, grab trip_key, feed_key, 
    stop_key, stop_time_key.
    """
    trip_keys_for_day = gtfs_utils.get_trips(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        trip_cols = ["trip_key"],
        get_df = False
    )
    
    ix_keys_for_day = (tbl.views.gtfs_schedule_index_feed_trip_stops()
                       >> inner_join(_, 
                                     trip_keys_for_day,
                                     on = "trip_key"
                                    )
                       >> select(-_.calitp_extracted_at, -_.calitp_deleted_at)
                       >> distinct()
    )
    
    return ix_keys_for_day


def get_routelines(itp_id: int, 
                   analysis_date: dt.date):
    """
    Download the route shapes (line geom) from dim_shapes_geo
    associated with shape_ids / trips that ran on selected day.
    
    Write gpd.GeoDataFrame to GCS.
    """
    dataset = "routelines"
    filename = f'{dataset}_{itp_id}_{date_str}.parquet'
    
    routelines = gtfs_utils.get_route_shapes(
            selected_date = analysis_date,
            itp_id_list = [itp_id],
            get_df = True,
            CRS = geography_utils.CA_NAD83Albers
        )
    
    if not routelines.empty:
        utils.geoparquet_gcs_export(routelines, 
                            EXPORT_PATH, 
                            filename)

        print(f"{itp_id}: {dataset} exported to GCS")
    

def get_trips(itp_id: int, analysis_date: dt.date, 
              route_types: list = None):
    """
    Download the trips that ran on selected day.
    TODO: filter for route_types? Or do later?
    
    Write pd.DataFrame to GCS.
    """
    dataset = "trips"
    filename = f"{dataset}_{itp_id}_{date_str}.parquet"
    
    keep_trip_cols = [
            "calitp_itp_id", "calitp_url_number", 
            "service_date", "trip_key", "trip_id",
            "route_id", "direction_id", "shape_id",
            "calitp_extracted_at", "calitp_deleted_at"
        ]

    trips = gtfs_utils.get_trips(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        trip_cols = keep_trip_cols,
        get_df = True
    ).drop_duplicates(subset=["trip_id"])

    keep_route_cols = [
        "calitp_itp_id", 
        "route_id", "route_short_name", "route_long_name",
        "route_desc", "route_type"
    ]

    routes = gtfs_utils.get_route_info(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        route_cols = keep_route_cols,
        get_df = False
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
    
    
def get_stops(itp_id: int, analysis_date: dt.date):
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
    
    stops_query = gtfs_utils.get_stops(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        stop_cols = keep_stop_cols,
        get_df = True,
        CRS = geography_utils.CA_NAD83Albers
    )
    
    # Narrow down stops for ones that took place that day
    # Use ix_keys_for_day
    stop_keys_for_day = (ix_keys_for_day 
                         >> select(_.stop_key) 
                         >> distinct() 
                         >> collect()
                        )    
    
    stops = (stops_query 
            >> inner_join(_, stop_keys_for_day,
                         on = "stop_key")  
            # should be ok to drop duplicates, but must use stop_id for future joins...
            >> select(-_.stop_key)
            >> distinct(_.stop_id, _keep_all=True)
    )
    
    if not stops.empty:
        utils.geoparquet_gcs_export(stops, EXPORT_PATH, filename)
        print(f"{itp_id}: {dataset} exported to GCS")
