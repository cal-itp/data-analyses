"""
Create routes file with identifiers including
route_id, route_name, agency_id, agency_name.

Operator-routes in shapes.txt need route line geometry.
Operator-routes not in shapes.txt use stop sequence 
to generate route line geometry.
"""
import geopandas as gpd
import pandas as pd
import os

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)
pd.set_option("display.max_rows", 20)

from calitp.tables import tbl
from calitp import query_sql
from datetime import datetime
from siuba import *

import prep_data2 as prep_data
import utils

DATA_PATH = prep_data.DATA_PATH

def merge_shapes_to_routes(trips, routes):
    # Left only means in trips, but shape_id not found in shapes.txt
    # right only means in routes, but no route that has that shape_id 
    # We probably should keep how = "left"?
    # left only means we can assemble from stop sequence?
    m1 = pd.merge(
            trips,
            routes,
            on = ["calitp_itp_id", "shape_id"],
            how = "left",
            validate = "m:1",
            indicator=True
        )
    
    return m1


def routes_for_operators_in_shapes(merged_shapes_routes, route_info):
    # Attach route_id from gtfs_schedule.trips, using shape_id
    routes1 = (merged_shapes_routes[merged_shapes_routes._merge=="both"]
      .drop(columns = ["geometry", "_merge"])
      .reset_index(drop=True)
     )

    routes_part1 = prep_data.attach_route_name(routes1, route_info)
    
    return routes_part1


def routes_for_operators_notin_shapes(merged_shapes_routes, route_info):
    missing_shapes = (merged_shapes_routes[merged_shapes_routes._merge=="left_only"]
      .drop(columns = ["geometry", "_merge"])
      .reset_index(drop=True)
     )
    
    # Only grab trip info for the shape_ids that are missing, or, appear in missing_shapes
    trip_cols = ["calitp_itp_id", "route_id", "shape_id"]

    dim_trips = (tbl.views.gtfs_schedule_dim_trips()
                 # filter first to just the smaller set of IDs in missing_shapes
                 >> filter(_.calitp_itp_id.isin(missing_shapes.calitp_itp_id))
                 # Now find those shape_ids and trips associated
                 >> filter(_.shape_id.isin(missing_shapes.shape_id))
                 >> select(*trip_cols, _.trip_key)
                 >> distinct()
                )

    missing_trips = (
        tbl.views.gtfs_schedule_fact_daily_trips()
        >> filter(_.service_date == prep_data.SELECTED_DATE, 
               _.is_in_service==True)
        >> select(_.trip_key, _.trip_id)
        >> inner_join(_, dim_trips, on = "trip_key")
        >> distinct()
        >> collect()
    )
    
    # Since there are multiple trips, we'll sort the same way, and keep the first one
    group_cols = ["calitp_itp_id", "route_id", "shape_id"]
    missing_trips2 = (missing_trips.sort_values(group_cols + ["trip_id"])
                      .drop_duplicates(subset=group_cols)
                      .reset_index(drop=True)
    )
    
    stop_info_trips = (
        tbl.views.gtfs_schedule_dim_stop_times()
        >> filter(_.calitp_itp_id.isin(missing_trips2.calitp_itp_id))
        >> filter(_.trip_id.isin(missing_trips2.trip_id))
        >> distinct()
        >> inner_join(_,
                      tbl.views.gtfs_schedule_dim_stops(), 
                      on = ["calitp_itp_id", "stop_id"])
        >> select(_.calitp_itp_id, _.trip_id, 
                  _.stop_id, _.stop_sequence,
                  _.stop_lon, _.stop_lat)
        >> distinct()
        >> collect()
        # Want to merge back route_id on, but need to collect first
        >> inner_join(_, missing_trips2)
    )
    
    # Somehow, getting back some multiple points for same trip_id, stop_id
    group_cols = ["calitp_itp_id", "trip_id", "stop_id"]
    stop_info_trips = (stop_info_trips.sort_values(group_cols)
                       .drop_duplicates(subset=group_cols)
                       .reset_index(drop=True)
                       # make_routes_line_geom_for_missing_shapes requires calitp_url_number
                       .assign(calitp_url_number=0)
                      )
    
    # Assemble line geometry
    missing_routes = utils.make_routes_line_geom_for_missing_shapes(stop_info_trips)
    
    # Merge route_id back in, which is lost when it 
    # passes through make_routes_line_geom_for_missing_shapes
    # Also, get rid of calitp_url_number
    missing_routes2 = pd.merge(
        missing_routes.drop(columns="calitp_url_number"),
        stop_info_trips[["calitp_itp_id", "shape_id", "route_id"]].drop_duplicates(),
        on = ["calitp_itp_id", "shape_id"],
        how = "inner",
        validate = "1:1",
    )
    
    routes_part2 = prep_data2.attach_route_name(missing_routes2, route_info)

    return routes_part2


# Assemble routes file
def make_routes_shapefile():
    time0 = datetime.now()
        
    # Read in local parquets
    stops = pd.read_parquet(f"{DATA_PATH}stops.parquet")
    trips = pd.read_parquet(f"{DATA_PATH}trips.parquet")
    route_info = pd.read_parquet(f"{DATA_PATH}route_info.parquet")
    routes = gpd.read_parquet(f"{DATA_PATH}routes.parquet")
    latest_itp_id = pd.read_parquet(f"{DATA_PATH}latest_itp_id.parquet")

    df = merge_shapes_to_routes(trips, routes)
    
    time1 = datetime.now()
    print(f"Read in data and merge shapes to routes: {time1-time0}")    
    
    routes_part1 = routes_for_operators_in_shapes(df, route_info)
    
    time2 = datetime.now()
    print(f"Part 1 - routes for operator-routes in shapes.txt: {time2-time1}")
    
    routes_part2 = routes_for_operators_notin_shapes(df, route_info)
    
    time3 = datetime.now()
    print(f"Part 2 - routes for operator-routes not in shapes.txt: {time3-time2}")
    
    routes_assembled = (routes_part1.append(routes_part2)
                        .sort_values(["calitp_itp_id", "route_id"])
                        .drop_duplicates()
                        .reset_index(drop=True)
                       )
    
    # Attach agency_id and agency_name using calitp_itp_id
    routes_assembled2 = prep_data.attach_agency_info(routes_assembled, agencies)
    
    routes_assembled2 = (prep_data.filter_latest_itp_id(routes_assembled2, 
                                                        latest_itp_id, 
                                                        itp_id_col = "calitp_itp_id")
                         # Any renaming to be done before exporting
                         .rename(columns = prep_dat2a.RENAME_COLS)
                         .sort_values(["itp_id", "route_id"])
                         .reset_index(drop=True)
                        )
    
    print(f"Routes script total execution time: {time3-time0}")

    return routes_assembled2
