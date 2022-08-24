import datetime as dt
import geopandas as gpd
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(130_000_000_000)

from calitp.tables import tbl
from siuba import *

import utils
import shared_utils

'''
Stash datasets in GCS to read in create_service_estimator.py
warehouse_queries and create_analysis_data functions related 
to service increase estimation need to be created in a "bundle"
otherewise, `shape_id` may not match
Use sub-folders to store intermediate parquets

The original analysis was done on Oct 2021 service dates for 
service estimation and tract bus arrivals.
Keep those in `utils.GCS_FILE_PATH`.

New sub-folders take form: f"{utils.GCS_FILE_PATH}SUBFOLDER_NAME/"
'''

DATA_PATH = f"{utils.GCS_FILE_PATH}2022_Jan/"

#---------------------------------------------------------------#
# Set dates for analysis
#---------------------------------------------------------------#
# Replace get_recent_dates()
# Explicitly set dates
dates = {
    #'thurs': dt.date(2021, 10, 7),
    #'sat': dt.date(2021, 10, 9),
    #'sun': dt.date(2021, 10, 10),
    "thurs": "2022-1-6", 
    "sat": "2022-1-8",
    "sun": "2022-1-9",
}

min_date = min(dates.values())
max_date = max(dates.values())


#---------------------------------------------------------------#
# Warehouse Queries for A1_generate_existing_service.ipynb
#---------------------------------------------------------------#
def grab_selected_trips_for_date(selected_date):
    ## get trips for operator on dates of interest, join with day of week table
    keep_trip_cols = [
        "calitp_itp_id", "service_date", 
        "trip_key", "trip_id", "is_in_service"
    ]
    
    trips = shared_utils.gtfs_utils.get_trips(
        selected_date = selected_date,
        itp_id_list = None,
        trip_cols = keep_trip_cols,
        get_df = True
    )
    
    ## get stop times for operator
    keep_stop_time_cols = [
        "calitp_itp_id", "trip_id", "departure_time", 
        "stop_sequence", "stop_id"
    ]
    
    stop_times = shared_utils.gtfs_utils.get_stop_times(
        selected_date = selected_date,
        itp_id_list = None,
        stop_time_cols = keep_stop_time_cols,
        get_df = True,
        trip_df = trips
    )
    
    # Join trips to stop times, which gives stop sequence and departure time
    trips_to_stops = pd.merge(
        trips,
        stop_times,
        on = ["calitp_itp_id", "trip_id"],
        how = "inner"
    )
    
    # Add in day of week
    date_tbl = (tbl.views.dim_date() 
                >> filter(_.full_date == selected_date)
                >> select(_.date == _.full_date, _.day_name)  
                >> collect()
           )
    
    trips_joined = pd.merge(
        trips_to_stops.rename(columns = {"service_date": "date"}),
        date_tbl,
        on = "date",
        how = "inner"
    )
    
    day_name = trips_joined.day_name.iloc[0].lower()
    
    three_letters = ["monday", "wednesday", "friday", "saturday", "sunday"]
    if day_name == "thursday":
        day = "thurs"
    elif day_name == "tuesday":
        day = "tues"
    elif day_name in three_letters:
        day = day_name[:3]

    trips_joined.to_parquet(f"{DATA_PATH}trips_joined_{day}.parquet")


#---------------------------------------------------------------#
# Warehouse Queries for B1_service_opportunities_tract.ipynb
#---------------------------------------------------------------#
def calculate_arrivals_at_stop(day_of_week: str = "thurs", 
                               selected_date: str = dates["thurs"]):
    # Since this is run for a selected date, don't need to rerun
    
    trips_on_day = pd.read_parquet(f"{DATA_PATH}trips_joined_{day_of_week}.parquet")
     
    # this handles multiple url_feeds already, finds distinct in dim_stop_times
    stop_times = shared_utils.gtfs_utils.get_stop_times(
        selected_date = selected_date,
        itp_id_list = None,
        get_df = False, # return dask df
        trip_df = trips_on_day,
        departure_hours = (5, 22)
    )
    
    # Aggregate to count daily stop times
    # Count the number of times a bus arrives at that stop daily
    daily_stop_times = (
        stop_times.groupby(["calitp_itp_id", "stop_id"])
        .agg({"arrival_time": "count"})
        .reset_index()
        .rename(columns = {"calitp_itp_id": "itp_id", 
                           "arrival_time": "num_arrivals"})
    ).compute()


    daily_stop_times.to_parquet(f"{utils.GCS_FILE_PATH}daily_stop_times.parquet")


def process_daily_stop_times(selected_date):
    daily_stop_times = pd.read_parquet(f"{utils.GCS_FILE_PATH}daily_stop_times.parquet")
    
    # Handle some exclusions
    daily_stop_times = daily_stop_times[daily_stop_times.calitp_itp_id != 200]

    # Add lat/lon info in
    keep_stop_cols = [
        "calitp_itp_id", "stop_id", "stop_name",
        "stop_lon", "stop_lat",
     ]
    
    stop_geom = shared_utils.gtfs_utils.get_stops(
        selected_date = selected_date,
        itp_id_list = None,
        stop_cols = keep_stop_cols,
        get_df = True,
    )
    
    aggregated_stops_with_geom = pd.merge(
        stop_geom.rename(columns = {"calitp_itp_id": "itp_id"}),
        daily_stop_times,
        on = ["itp_id", "stop_id"]
    )

    aggregated_stops_with_geom.to_parquet(
        f"{utils.GCS_FILE_PATH}aggregated_stops_with_geom.parquet")
    
    
if __name__ == "__main__":
    # Run this to get the static parquet files
    # Analysis is for a particular day, so don't need to hit warehouse constantly
    
    # (1) Get existing service 
    for key in dates.keys():
        print(f"Grab selected trips for {key}")
        selected_date = dates[key]
        grab_selected_trips_for_date(selected_date)
    
    # (2) Get daily bus stop arrivals with geometry
    # Only do it for a weekday (Thurs)
    day_of_week = "thurs"
    calculate_arrivals_at_stop(day_of_week, dates[day_of_week])
    process_daily_stop_times(dates[day_of_week])