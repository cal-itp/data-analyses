import geopandas as gpd
import os
import pandas as pd

import utils
import shared_utils

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)
pd.set_option("display.max_rows", 20)

from calitp.tables import tbl
from calitp import query_sql
from siuba import *

SELECTED_DATE = "2021-10-07"

#---------------------------------------------------------------#
# Warehouse Queries for B1_service_opportunities_tract.ipynb
#---------------------------------------------------------------#
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

daily_stop_times.to_parquet(f"{utils.DATA_PATH}daily_stop_times.parquet")
'''

def process_daily_stop_times():
    daily_stop_times = pd.read_parquet(f"{utils.DATA_PATH}daily_stop_times.parquet")
    
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

    aggregated_stops_with_geom.to_parquet(f"{utils.DATA_PATH}aggregated_stops_with_geom.parquet")
    
    
if __name__ == "__main__":
    # Run this to get the static parquet files
    # Analysis is for a particular day, so don't need to hit warehouse constantly
    
    # Put A1_generate_existing_service.ipynb queries here
    # Get existing service 
    
    # Get daily bus stop arrivals with geometry
    process_daily_stop_times()