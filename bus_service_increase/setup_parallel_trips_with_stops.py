import geopandas as gpd
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from calitp import query_sql
from siuba import *

import utils
import shared_utils


def grab_service_hours(df, SELECTED_DATE):
    daily_trip_info = (
        tbl.views.gtfs_schedule_fact_daily_trips()
        >> filter(_.service_date == SELECTED_DATE)
        >> select(_.calitp_itp_id, 
               _.trip_key, _.service_hours, 
               _.trip_first_departure_ts, _.trip_last_arrival_ts
              ) 
        >> collect()
    )
    
    df2 = pd.merge(df, 
                   daily_trip_info,
                   on = ["calitp_itp_id", "trip_key"],
                   how = "inner",
                   # m:1 because trips has stop_level data by trips
                   # 1 is on the right beause service_hours is trip-level
                   validate = "m:1"
                  )
    
    return df2

def select_one_trip(df):
    drop_cols = ["stop_sequence", "stop_id", "departure_time", 
                 "trip_first_departure_ts", "trip_last_arrival_ts", 
                ]
    

    df = df.assign(
        departure_hr = pd.to_datetime(df.trip_first_departure_ts, unit='s').dt.hour                                        
    ).drop(columns = drop_cols).drop_duplicates().reset_index(drop=True)
    
    # Across trip_ids, for the same route_id, there are differing max_stop_sequence
    # Can't use max_stop_sequence to find longest route_length
    # Use service hours instead to find faster trip during free-flowing traffic
    group_cols = ["calitp_itp_id", "route_id"]
    
    # Should there be a check that there are mid-day trips for that route_id?
    # Select trip by departure_hr
    hour_order = [
        12, 11, 13, 10, 14, # ideally we want mid-day
        15, 7, 20, 6, 21, # but, can move into earlier PM or early AM
        0, 1, 2, 3, 4, 5, 22, 23, # owl service
        8, 9, # AM peak 
        16, 17, 18, 19, # PM peak
    ]
    
    for i in range(0, 24):
        if i == 0:
            df['selection_rank'] = df.apply(
                lambda x: hour_order[i] if x.departure_hr == i 
                else 0, axis=1) 
        else:
            df['selection_rank'] = df.apply(
                lambda x: hour_order[i] if x.departure_hr == i 
                else x.selection_rank, axis=1) 
    
    # Select a trip that closest to 25th percentile (lower means faster!)
    # This groupby ruins the index, throws an error, so just merge in as separate df
    quantile = (df.groupby(group_cols)["service_hours"]
                .quantile(0.25).reset_index()
                .rename(columns = {"service_hours": "p25"})
               )
    
    df = pd.merge(df, quantile, 
                  on = group_cols,
                  how = "inner",
                  validate = "m:1"
            )
    
    # Select trip that is closest to 25th percentile (min_diff)
    df["difference"] = df.service_hours - df.p25
    df["min_diff"] = df.groupby(group_cols)["difference"].transform("min")

    df['faster_trip'] = df.apply(lambda x: 
                                 1 if x.difference == x.min_diff else 0, axis=1)
    
    # If there are multiple trips selected for a route, do a sort/drop duplicates
    # This df is trip-level (no stop_id, becuase that was dropped at beginning)
    df2 = (df[df.faster_trip==1]
           .sort_values(group_cols + ["departure_hr"], 
                        # If there are multiple trips with same service hours, 
                        # pick one with later departure hr (closer to mid-day)
                        ascending=[True, True, False])
           .drop_duplicates(subset=group_cols)
           .drop(columns = ["faster_trip", "difference", "min_diff", 
                            "p25", "selection_rank"])
           .reset_index(drop=True)
          )

    return df2


def subset_to_parallel_routes(df):
    # Just use route_id to flag parallel, not shape_id
    # It won't matter anyway, because we will use stop's point geom
    parallel_routes = shared_utils.utils.download_geoparquet(utils.GCS_FILE_PATH, 
                                             "parallel_or_intersecting")
    
    keep_cols = ["calitp_itp_id", "route_id", "geometry"]

    parallel_routes2 = (parallel_routes[parallel_routes.parallel==1]
           .reset_index(drop=True)
           .rename(columns = {"itp_id": "calitp_itp_id"})
           [keep_cols]
           .drop_duplicates()
           .reset_index(drop=True)
          )
    
    # Put parallel routes on the right because we don't need its line geometry
    gdf = pd.merge(
        df,
        parallel_routes2,
        on = ["calitp_itp_id", "route_id"],
        how = "inner",
        validate = "m:1",
    )
    
    return gdf


def grab_stop_geom(df):
    stop_info = (tbl.views.gtfs_schedule_dim_stops()
                 >> select(_.calitp_itp_id,
                       _.stop_id, _.stop_lon, _.stop_lat,
                      )
             >> distinct()
             >> collect()
    )
    
    df2 = pd.merge(
        df,
        (stop_info.sort_values(["calitp_itp_id", "stop_id", "stop_lon"])
         .drop_duplicates(subset=["calitp_itp_id", "stop_id"])
        ),
        on = ["calitp_itp_id", "stop_id"],
        how = "inner", 
        validate = "m:1"
    )
    
    gdf = (shared_utils.geography_utils.create_point_geometry(df2)
           .sort_values(["calitp_itp_id", "route_id", 
                         "trip_id", "stop_sequence"])
           .reset_index(drop=True)
           .drop(columns = ["stop_lon", "stop_lat", 
                            "trip_first_departure_ts", "trip_last_arrival_ts"])
          )

    return gdf


def make_parallel_routes_df_with_stops(): 
    DATA_PATH = f"{utils.GCS_FILE_PATH}2022_Jan/"

    # Read in intermediate parquet for trips on selected date
    trips = pd.read_parquet(f"{DATA_PATH}trips_joined_thurs.parquet")
    SELECTED_DATE = '2022-1-6' #warehouse_queries.dates['thurs']
    
    # Attach service hours
    # This df is trip_id-stop_id level
    trips_with_service_hrs = grab_service_hours(trips, SELECTED_DATE)

    # Narrow down to 1 trip per route_id
    selected_trip = select_one_trip(trips_with_service_hrs)
    
    # Narrow down to just parallel routes
    selected_parallel_trips = subset_to_parallel_routes(selected_trip)

    # Recall: trips_with_service_hrs contains stop_id
    # selected_parallel_trips is trip-level
    # Pare down trips_with_service_hrs with isin()
    parallel_trips_with_stops = (
        trips_with_service_hrs[
                trips_with_service_hrs.trip_key.isin(selected_parallel_trips.trip_key)]
              .reset_index(drop=True)
             )

    final_df = grab_stop_geom(parallel_trips_with_stops)
    final_df.to_parquet("./data/parallel_trips_with_stops.parquet")
    
make_parallel_routes_df_with_stops() 