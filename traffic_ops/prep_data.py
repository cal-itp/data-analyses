"""
Functions to query GTFS schedule data, 
save locally as parquets, 
then clean up at the end of the script.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import glob
import os

from shared_utils import geography_utils, portfolio_utils, gtfs_utils

ANALYSIS_DATE = datetime.date(2022, 7, 13)

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/traffic_ops/"
DATA_PATH = "./data/"
    
stop_cols = ["calitp_itp_id", "stop_id", 
             "stop_name", "stop_code", 
             "geometry"
            ]

trip_cols = ["calitp_itp_id", "route_id", "shape_id"]


def grab_selected_date(selected_date: datetime.date | str) 
-> tuple[gpd.GeoDataFrame, pd.DataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    """
    Return all the cached files for stops, trips, routes, and route_info
    """
    stops = gtfs_utils.all_routelines_or_stops_with_cached(
        dataset = "stops",
        analysis_date = selected_date,
    )[stop_cols]
    
    trips = gtfs_utils.all_trips_or_stoptimes_with_cached(
        dataset = "trips",
        analysis_date = selected_date
    )[trip_cols]
    
    routes = gtfs_utils.all_routelines_or_stops_with_cached(
        dataset = "routelines",
        analysis_date = selected_date
    ).to_crs(geography_utils.WGS84)
    
    route_info = portfolio_utils.add_route_name(selected_date)
    
    return stops, trips, routes, route_info
    

def create_local_parquets(selected_date: datetime.date):
    """
    Save parquets locally while analysis is running
    """
    time0 = datetime.now()
    stops, trips, routes, route_info = grab_selected_date(selected_date)    
    
    stops.to_parquet(f"{DATA_PATH}stops.parquet")
    trips.to_parquet(f"{DATA_PATH}trips.parquet")
    routes.to_parquet(f"{DATA_PATH}routes.parquet")
    route_info.to_parquet(f"{DATA_PATH}route_info.parquet")
    
    time1 = datetime.now()
    print(f"Part 1: Queries and create local parquets: {time1-time0}")
            
    print(f"Total execution time: {time1-time0}") 
    
    
# Function to delete local parquets
def delete_local_parquets():
    # Find all local parquets
    FILES = [f for f in glob.glob(f"{DATA_PATH}*.parquet")]
    print(f"list of parquet files to delete: {FILES}")
    
    for file_name in FILES:
        os.remove(f"{file_name}")


#----------------------------------------------------#        
# Functions are used in 
# `create_routes_data.py` and `create_stops_data.py`
#----------------------------------------------------#
# Define column names, must fit ESRI 10 character limits
RENAME_COLS = {
    "calitp_itp_id": "itp_id",
    "calitp_agency_name": "agency",
    "route_name_used": "route_name",
}


def attach_route_name(df: pd.DataFrame, route_info_df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to attach route_info using route_id

    Parameters:
    df: pandas.DataFrame
        each row is unique to itp_id-route_id
    route_info_df: pandas.DataFrame
                    each row is unique to route_id-route_name_used
                    portfolio_utils selects 1 from route_short_name, 
                    route_long_name, and route_desc
    """
    # Attach route info from gtfs_schedule.routes, using route_id
    route_info_unique = (route_info_df
                         .sort_values(["calitp_itp_id", "route_id", "route_name_used"])
                         .drop_duplicates(subset=["calitp_itp_id", "route_id"])
                        )
    
    routes = dd.merge(
        df, 
        route_info_unique,
        on = ["calitp_itp_id", "route_id"],
        how = "left",
        #validate = "m:1",
    )

    return routes