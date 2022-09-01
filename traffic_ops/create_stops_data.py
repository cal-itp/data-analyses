"""
Create stops file with identifiers including
route_id, route_name, agency_id, agency_name.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from calitp.tables import tbl
from datetime import datetime
from siuba import *

import prep_data
from shared_utils import geography_utils, gtfs_utils, portfolio_utils, rt_utils
from create_routes_data import add_route_agency_name, remove_trip_cols


# Attach all the various route information    
def attach_route_info_to_stops(stops: dg.GeoDataFrame, 
                               trips: dg.GeoDataFrame, 
                               stop_times: dd.DataFrame
                              ) -> gpd.GeoDataFrame:
    
    # Keep the route characteristics associated with trip_id
    trips2 = (trips.drop(columns = remove_trip_cols)
          .drop_duplicates(subset=["calitp_itp_id", "trip_id"])
          .reset_index(drop=True)
         )
        
    # In stop_times table, find the trip ids that are present that day
    # then find unique stop_ids present that day
    stop_ids_on_day = (dd.merge(
                stop_times,
                trips2[["calitp_itp_id", "trip_id"]].drop_duplicates(), 
                on = ["calitp_itp_id", "trip_id"]
            ).drop_duplicates(subset=["calitp_itp_id", "stop_id"])
            .reset_index(drop=True)
            [["calitp_itp_id", "stop_id", "trip_id"]]
        )
    
    # Pare down stops to just stops that show up on the day 
    stops_on_day = dd.merge(
        stops.drop(columns = "stop_key"), 
        stop_ids_on_day,
        on = ["calitp_itp_id", "stop_id"],
        how = "inner"
    )
    
    # Merge in route info from trips table
    keep_cols = [
        'calitp_itp_id', 'stop_id', 'stop_name', 'geometry',
        'route_id', 'route_type'
    ]
    
    stops_with_route_info = dd.merge(
        stops_on_day,
        trips2, 
        on = ["calitp_itp_id", "trip_id"],
        how = "inner"
    )[keep_cols].to_crs(geography_utils.WGS84).compute()
    
    stops_with_names = add_route_agency_name(stops_with_route_info)

    return stops_with_names


def make_stops_shapefile():
    time0 = datetime.now()

    # Read in local parquets
    stops = dg.read_parquet(
        f"{prep_data.GCS_FILE_PATH}stops_{prep_data.ANALYSIS_DATE}_all.parquet")
    trips = dd.read_parquet(
        f"{prep_data.GCS_FILE_PATH}trips_{prep_data.ANALYSIS_DATE}_all.parquet")
    stop_times = dd.read_parquet(
        f"{prep_data.GCS_FILE_PATH}st_{prep_data.ANALYSIS_DATE}_all.parquet")
    
    time1 = datetime.now()
    print(f"Get rid of duplicates: {time1-time0}")
    
    df = attach_route_info_to_stops(stops, trips, stop_times)
    
    time2 = datetime.now()
    print(f"Attach route and operator info to stops: {time2-time1}")
    
    stops_assembled = (df.sort_values(["itp_id", "route_id", "stop_id"])
                       .reset_index(drop=True)
    )
    
    print(f"Stops script total execution time: {time2-time0}")
    
    return stops_assembled