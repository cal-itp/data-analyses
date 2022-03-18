"""
REWORK traffic_ops/prep_data.py
CAN HISTORICAL AND CURRENT QUERIES BE DEFINED


Functions to query GTFS schedule data, 
save locally as parquets, 
then clean up at the end of the script.
"""
import pandas as pd
import os

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from calitp import query_sql
from siuba import *



stop_cols = ["calitp_itp_id", "stop_id", 
             "stop_lat", "stop_lon", 
             "stop_name", "stop_code"
            ]

trip_cols = ["calitp_itp_id", "route_id", "shape_id"]
route_cols = ["calitp_itp_id", "route_id", 
              "route_short_name", "route_long_name"]


def grab_current_data():
    stops = (tbl.gtfs_schedule.stops()
             >> select(*stop_cols)
             >> distinct()
            )
    
    trips = (tbl.gtfs_schedule.trips()
             >> select(*trip_cols)
             >> distinct()
             >> collect()
            )

    route_info = (tbl.gtfs_schedule.routes()
    # NB/SB may share same route_id, but different short/long names
                  >> select(*route_cols)
                  >> distinct()
                  >> collect()
                 )
    
    return stops, trips, route_info


def grab_selected_date(SELECTED_DATE):
    # Stops query
    dim_stops = (tbl.views.gtfs_schedule_dim_stops()
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
    dim_routes = (tbl.views.gtfs_schedule_dim_routes()
                  >> select(*route_cols, _.route_key)
                  >> distinct()
                 )


    route_info = (tbl.views.gtfs_schedule_fact_daily_feed_routes()
                  >> filter(_.date == SELECTED_DATE)
                  >> select(_.route_key, _.date)
                  >> inner_join(_, dim_routes, on = "route_key")
                  >> select(*route_cols)
                  >> distinct()
                  >> collect()
                 )
    
    return stops, trips, route_info



def create_local_parquets(SELECTED_DATE = None):
    if SELECTED_DATE is None:
        stops, trips, route_info = grab_current_data()
    else:
        stops, trips, route_info = grab_selected_date(SELECTED_DATE)
    
    