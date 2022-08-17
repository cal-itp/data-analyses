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
from calitp import query_sql
from datetime import datetime
from siuba import *

import prep_data
from shared_utils import geography_utils, portfolio_utils


# Attach all the various route information    
def attach_route_info_to_stops(stops: dg.GeoDataFrame, 
                               trips: dg.GeoDataFrame) -> dg.GeoDataFrame:
    # From trip table, we have trip_key
    # Compare this against the index table, where we have trip_key and stop_key
    trip_keys_on_day = list(trips.trip_key.unique())

    ix_trips = (tbl.views.gtfs_schedule_index_feed_trip_stops()
                >> filter(_.trip_key.isin(trip_keys_on_day))
                >> select(_.trip_key, _.stop_key)
                >> collect()
               )
    
    # By adding in trip_key to stops, we can join in route_info from our trips table
    stops_on_day = dd.merge(
        stops, 
        ix_trips,
        on = "stop_key",
        how = "inner"
    ).merge(trips, 
            on = ["calitp_itp_id", "trip_key"],
            how = "inner"
    )
    
    route_name_used = portfolio_utils.add_route_name(SELECTED_DATE = prep_data.ANALYSIS_DATE)
    
    keep_cols = [
        'calitp_itp_id', 'stop_id', 'stop_name', 'geometry',
        'route_id', 'route_name_used',
    ]
    
    stops_with_route_names = dd.merge(
        stops_on_day,
        route_name_used,
        on = ["calitp_itp_id", "route_id"],
        how = "inner",
    )[keep_cols]
    
    # Attach agency_name
    agency_names = portfolio_utils.add_agency_name(
        SELECTED_DATE = prep_data.ANALYSIS_DATE)
    
    stops_with_names = dd.merge(
        stops_with_route_names,
        agency_names,
        on = "calitp_itp_id",
        how = "left",
        #validate = "m:1"
    )
    
    stops_cleaned = stops.to_crs(geography_utils.WGS84).compute()
    
    return stops_cleaned


def make_stops_shapefile():
    time0 = datetime.now()

    # Read in local parquets
    stops = dg.read_parquet(f"{prep_data.GCS_FILE_PATH}stops.parquet")
    trips = dd.read_parquet(f"{prep_data.GCS_FILE_PATH}trips.parquet")
        
    time1 = datetime.now()
    print(f"Get rid of duplicates: {time1-time0}")
    
    df = attach_route_info_to_stops(stops, trips)
    
    time2 = datetime.now()
    print(f"Attach route and operator info to stops: {time2-time1}")
    
    latest_itp_id = portfolio_utils.latest_itp_id()

    df2 = (pd.merge(df, 
                    latest_itp_id,
                    on = "calitp_itp_id",
                    how = "inner")
           # Any renaming to be done before exporting
           .rename(columns = prep_data.RENAME_COLS)
           .sort_values(["itp_id", "route_id", "stop_id"])
           .reset_index(drop=True)
          )
    
    print(f"Stops script total execution time: {time2-time0}")
    
    return df2