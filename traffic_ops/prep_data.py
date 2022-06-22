"""
Functions to query GTFS schedule data, 
save locally as parquets, 
then clean up at the end of the script.
"""
import geopandas as gpd
import pandas as pd
import glob
import os

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from calitp import query_sql
from datetime import datetime, date, timedelta
from siuba import *

from shared_utils import geography_utils, portfolio_utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/traffic_ops/"
DATA_PATH = "./data/"
    
#SELECTED_DATE = date.today() - timedelta(days=1)
SELECTED_DATE = "2022-06-21"

stop_cols = ["calitp_itp_id", "stop_id", 
             "stop_lat", "stop_lon", 
             "stop_name", "stop_code"
            ]

trip_cols = ["calitp_itp_id", "route_id", "shape_id"]


def grab_selected_date(SELECTED_DATE):
    # Always exclude ITP_ID = 200!
    # Stops query
    dim_stops = (tbl.views.gtfs_schedule_dim_stops()
                 >> filter(_.calitp_itp_id != 200, _.calitp_itp_id != 0)
                 >> select(*stop_cols, _.stop_key)
                 >> distinct()
                )

    stops = (tbl.views.gtfs_schedule_fact_daily_feed_stops()
             >> filter(_.date == SELECTED_DATE)
             >> select(_.stop_key, _.date)
             >> inner_join(_, dim_stops, on = "stop_key")
             >> select(*stop_cols)
             >> distinct()
             >> collect()
            )
    
    # Trips query
    dim_trips = (tbl.views.gtfs_schedule_dim_trips()
                >> filter(_.calitp_itp_id != 200, _.calitp_itp_id != 0)
                 >> select(*trip_cols, _.trip_key)
                 >> distinct()
                )

    trips = (tbl.views.gtfs_schedule_fact_daily_trips()
             >> filter(_.service_date == SELECTED_DATE, 
                       _.is_in_service==True)
             >> select(_.trip_key, _.service_date)
             >> inner_join(_, dim_trips, on = "trip_key")
             >> select(*trip_cols)
             >> distinct()
             >> collect()
            )
    
    ## Route info query
    route_info = portfolio_utils.add_route_name(SELECTED_DATE)
    
    return stops, trips, route_info


def create_local_parquets(SELECTED_DATE):
    time0 = datetime.now()
    stops, trips, route_info = grab_selected_date(SELECTED_DATE)
    
    # Filter to the ITP_IDs present in the latest agencies.yml
    latest_itp_id = (tbl.views.gtfs_schedule_dim_feeds()
                     >> filter(_.calitp_id_in_latest==True)
                     >> select(_.calitp_itp_id)
                     >> distinct()
                     >> collect()
                    )
    
    stops.to_parquet(f"{DATA_PATH}stops.parquet")
    trips.to_parquet(f"{DATA_PATH}trips.parquet")
    route_info.to_parquet(f"{DATA_PATH}route_info.parquet")
    latest_itp_id.to_parquet(f"{DATA_PATH}latest_itp_id.parquet")
    
    time1 = datetime.now()
    print(f"Part 1: Queries and create local parquets: {time1-time0}")

    routes = geography_utils.make_routes_gdf(SELECTED_DATE, 
                                             CRS=geography_utils.WGS84, 
                                             ITP_ID_LIST=None)
    routes_unique = (routes[(routes.calitp_itp_id != 0) & 
                            (routes.calitp_itp_id != 20)]
                     .sort_values(["calitp_itp_id", "calitp_url_number", "shape_id"])
                     .drop_duplicates(subset=["calitp_itp_id", "shape_id"])
                     .drop(columns = ["calitp_url_number", "pt_array"])
                     .sort_values(["calitp_itp_id", "shape_id"])
                     .reset_index(drop=True)
    )
    routes_unique.to_parquet(f"{DATA_PATH}routes.parquet")
    
    time2 = datetime.now()
    print(f"Part 2: Shapes: {time2-time1}")
            
    print(f"Total execution time: {time2-time0}") 
    
    
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
    "calitp_agency_name": "agency_name",
}


# Define function to attach route_info using route_id
def attach_route_name(df, route_info_df):
    """
    Parameters:
    df: pandas.DataFrame
        each row is unique to itp_id-route_id
    route_info_df: pandas.DataFrame
                    each row is unique to route_id-route_name_used
                    portfolio_utils selects 1 from route_short_name, route_long_name, and route_desc
    """
    # Attach route info from gtfs_schedule.routes, using route_id
    route_info_unique = (route_info_df
                         .sort_values(["calitp_itp_id", "route_id", "route_name_used"])
                         .drop_duplicates(subset=["calitp_itp_id", "route_id"])
                        )
    
    routes = pd.merge(
        df, 
        route_info_unique,
        on = ["calitp_itp_id", "route_id"],
        how = "left",
        validate = "m:1",
    )

    return routes


# Function to filter to latest ITP_ID in agencies.yml
def filter_latest_itp_id(df, latest_itp_id_df, itp_id_col = "calitp_itp_id"):
    starting_length = len(df)
    print(f"# rows to start: {starting_length}")
    print(f"# operators to start: {df[itp_id_col].nunique()}")
    
    # Drop ITP_IDs if not found in the latest_itp_id
    if itp_id_col != "calitp_itp_id":
        latest_itp_id_df = latest_itp_id_df.rename(columns = {
            "calitp_itp_id": itp_id_col})
    
    df = (df[df[itp_id_col].isin(latest_itp_id_df[itp_id_col])]
          .reset_index(drop=True)
         )
        
    only_latest_id = len(df)
    print(f"# rows with only latest agencies.yml: {only_latest_id}")
    print(f"# operators with only latest agencies.yml: {df[itp_id_col].nunique()}")
    print(f"# rows dropped: {only_latest_id - starting_length}")
    
    return df
