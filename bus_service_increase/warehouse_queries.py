import datetime as dt
import geopandas as gpd
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(130_000_000_000)

from calitp.tables import tbl
from calitp import query_sql
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
date_tbl = (tbl.views.dim_date() 
            >> select(_.date == _.full_date, _.day_name)            
           )

def grab_selected_trips_for_date(selected_date):
    # Maybe do away with the collect(), that slows it down a lot
    # Have a way to merge the LzyTbls with joins, at the end, collect()
    
    ## get trips for operator on dates of interest, join with day of week table
    trips = (tbl.views.gtfs_schedule_fact_daily_trips()
             >> filter(_.calitp_extracted_at <= min_date, 
                       _.calitp_deleted_at > max_date)
             >> filter(_.is_in_service == True, _.service_date == selected_date)
             >> select(_.calitp_itp_id, _.date == _.service_date, 
                       _.trip_key, _.trip_id, _.is_in_service)
             >> inner_join(_, date_tbl, on = 'date')
    )
    
    ## get stop times for operator
    tbl_stop_times = (tbl.views.gtfs_schedule_dim_stop_times()
                      >> filter(_.calitp_extracted_at <= min_date, 
                                _.calitp_deleted_at > max_date)
                      >> select(_.calitp_itp_id, _.trip_id, _.departure_time,
                                _.stop_sequence, _.stop_id)
    )
    
    ## join stop times to trips 
    stops = (trips 
             >> inner_join(_, tbl_stop_times, on = ['calitp_itp_id', 'trip_id'])
            )
    
    ## get trips dimensional table
    tbl_trips = (tbl.views.gtfs_schedule_dim_trips()
                 >> filter(_.calitp_extracted_at <= min_date, 
                           _.calitp_deleted_at > max_date)
                 >> select(_.trip_key, _.shape_id, _.route_id)
    )

    ## join dim_trips info to stop times
    trips_joined = (stops 
                    >> inner_join(_, tbl_trips, on = 'trip_key')
                    >> collect()
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
SELECTED_DATE = dates['thurs']

'''
# Since this is run for a selected date, don't need to rerun

tbl_stop_times = (
    tbl.views.gtfs_schedule_dim_stop_times()
    >> filter(_.calitp_extracted_at <= SELECTED_DATE, 
              _.calitp_deleted_at > SELECTED_DATE, 
             )
)


daily_stop_times = (
    tbl.views.gtfs_schedule_fact_daily_trips()
 >> filter(_.service_date == SELECTED_DATE, 
          _.is_in_service == True)
 >> left_join(_, tbl_stop_times,
              # also added url number to the join keys ----
             ["calitp_itp_id", "calitp_url_number", "trip_id"])
 >> select(_.itp_id==_.calitp_itp_id, _.calitp_url_number,
           _.trip_key, _.trip_id, 
           _.service_date,
           _.stop_id, _.stop_sequence, _.arrival_time)
 >> filter(_.arrival_time >= "05:00:00", 
          _.arrival_time <= "21:00:00")
 >> group_by(_.itp_id, _.calitp_url_number, 
             _.trip_id, _.trip_key,
             _.service_date, 
             _.stop_id, _.stop_sequence)
 >> count(_.arrival_time)
 >> collect()
)

daily_stop_times.to_parquet(f"{utils.GCS_FILE_PATH}daily_stop_times.parquet")
'''

def process_daily_stop_times():
    daily_stop_times = pd.read_parquet(f"{utils.GCS_FILE_PATH}daily_stop_times.parquet")
    
    # Handle some exclusions
    daily_stop_times = utils.include_exclude_multiple_feeds(
        daily_stop_times, id_col = "itp_id",
        include_ids = [182], exclude_ids = [200])

    # Count the number of times a bus arrives at that stop daily
    aggregated_stops_per_day = (daily_stop_times
                                .groupby(["itp_id", "stop_id"])
                                .agg({"arrival_time": "count"})
                                .reset_index()
                                .rename(columns = {"arrival_time": "num_arrivals"})
                               )

    # Add lat/lon info in
    aggregated_stops_with_geom = (
        tbl.views.gtfs_schedule_dim_stops()
        >> select(_.itp_id == _.calitp_itp_id, _.stop_id, 
                 _.stop_lat, _.stop_lon, _.stop_name)
        >> arrange(_.itp_id, _.stop_id, 
                   _.stop_lat, _.stop_lon)
        >> collect()
        >> inner_join(_, aggregated_stops_per_day, 
                  ["itp_id", "stop_id"])
        >> collect()
    )

    aggregated_stops_with_geom.to_parquet(f"{utils.GCS_FILE_PATH}aggregated_stops_with_geom.parquet")
    
    
if __name__ == "__main__":
    # Run this to get the static parquet files
    # Analysis is for a particular day, so don't need to hit warehouse constantly
    
    # (1) Get existing service 
    for key in dates.keys():
        print(f"Grab selected trips for {key}")
        selected_date = dates[key]
        grab_selected_trips_for_date(selected_date)
    
    # (2) Get daily bus stop arrivals with geometry
    process_daily_stop_times()