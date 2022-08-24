"""
Warehouse queries for regional bus network.

Amtrak thruway buses + local buses that run the same
origin/destination.

Adapt bus_service_increase/warehouse_queries.py
and traffic_ops/prep_data.py
"""
import datetime
import geopandas as gpd
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from siuba import *

from shared_utils import geography_utils

SELECTED_DATE = "2022-7-29"
itp_id = 13

stop_cols = ["calitp_itp_id", "stop_id", 
             "stop_lat", "stop_lon", 
             "stop_name", "stop_code"
            ]

trip_cols = ["calitp_itp_id", "route_id", "shape_id", "trip_id"]   

route_cols = ["calitp_itp_id", "route_id", 
              "route_type", "route_long_name"]

stop_time_cols = ["stop_id", 
                  "stop_sequence", "stop_sequence_rank", 
                  "departure_time"
                 ]

def get_routes(SELECTED_DATE: str | datetime.date, 
               itp_id: int) -> pd.DataFrame:
    dim_routes = (tbl.views.gtfs_schedule_dim_routes()
                  >> filter(_.calitp_itp_id == itp_id)
                  >> select(*route_cols + ["route_key"])
                  >> distinct() 
                 )
    
    routes = (tbl.views.gtfs_schedule_fact_daily_feed_routes()
              >> filter(_.date == SELECTED_DATE)
              >> select(_.route_key, _.date)
              >> inner_join(_, dim_routes, on = "route_key")
              >> select(*route_cols)
              >> distinct()
              >> collect()
             )

    return routes


def get_trips_with_stop_times(SELECTED_DATE: str | datetime.date, 
                              itp_id: int) -> pd.DataFrame:
    # Trips query
    dim_trips = (tbl.views.gtfs_schedule_dim_trips()
                >> filter(_.calitp_itp_id == itp_id)
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
            )
    
    
    # Join to stop_times, since stop_times always depends on trip_id
    # No other way to filter stop_times by date
    trips_with_stop_times = (tbl.views.gtfs_schedule_dim_stop_times()
                             >> filter(_.calitp_itp_id == itp_id)
                             >> inner_join(_, trips, 
                                           on = ["calitp_itp_id", "trip_id"])
                             >> select(*trip_cols, *stop_time_cols)
                             >> collect()
                            )
    
    return trips_with_stop_times


def get_stops(SELECTED_DATE: str | datetime.date, 
              itp_id: int) -> gpd.GeoDataFrame:
    # Stops query
    dim_stops = (tbl.views.gtfs_schedule_dim_stops()
                 >> filter(_.calitp_itp_id == itp_id)
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
    
    stops = (geography_utils.create_point_geometry(stops)
             .drop(columns = ["stop_lon", "stop_lat"])
            )
    
    return stops


def grab_selected_date(SELECTED_DATE: str | datetime.date, 
                       itp_id: int):
    routes = geography_utils.make_routes_gdf(SELECTED_DATE, 
                                ITP_ID_LIST = [itp_id]).drop(columns="pt_array")
    route_info = get_routes(SELECTED_DATE, itp_id)
    trips_with_stop_times = get_trips_with_stop_times(SELECTED_DATE, itp_id)
    stops = get_stops(SELECTED_DATE, itp_id)
    
    routes.to_parquet("./data/amtrak_routes.parquet")
    route_info.to_parquet("./data/amtrak_route_info.parquet")
    trips_with_stop_times.to_parquet("./data/amtrak_trips_with_stop_times.parquet")
    stops.to_parquet("./data/amtrak_stops.parquet")


if __name__=="__main__":
    
    grab_selected_date(SELECTED_DATE, itp_id)