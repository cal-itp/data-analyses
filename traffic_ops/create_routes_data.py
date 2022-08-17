"""
Create routes file with identifiers including
route_id, route_name, agency_id, agency_name.

Operator-routes in shapes.txt need route line geometry.
Operator-routes not in shapes.txt use stop sequence 
to generate route line geometry.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from calitp.tables import tbl
from datetime import datetime
from siuba import *

import prep_data
from shared_utils import geography_utils, gtfs_utils, portfolio_utils


def merge_trips_to_routes(trips: dd.DataFrame, 
                          routes: dg.GeoDataFrame) -> dg.GeoDataFrame:
    # Routes or trips can contain multiple calitp_url_numbers 
    # for same calitp_itp_id-shape_id. Drop these now
    # dask can only sort by 1 column!
    shape_id_cols = ["calitp_itp_id", "shape_id"]
    
    routes = (routes.sort_values("calitp_url_number")
              .drop_duplicates(subset=shape_id_cols)
              .reset_index(drop=True)
    )
    
    trips = (trips.sort_values("calitp_url_number")
             .drop_duplicates(subset=["calitp_itp_id", "trip_id"])
             .reset_index(drop=True)
    )
    
    # Left only means in trips, but shape_id not found in shapes.txt
    # right only means in routes, but no route that has that shape_id 
    # We probably should keep how = "left"?
    # left only means we can assemble from stop sequence?
    m1 = dd.merge(
            trips,
            routes[shape_id_cols + ["geometry"]].drop_duplicates(),
            on = shape_id_cols,
            how = "left",
            #validate = "m:1",
            indicator=True
        ).compute()

    # routes is a gdf, so turn it back into gdf
    m2 = gpd.GeoDataFrame(m1, geometry="geometry", 
                          crs = geography_utils.CA_NAD83Albers
                         ).to_crs(geography_utils.WGS84)
    
    m2 = dg.from_geopandas(m2, npartitions=1)
    
    return m2


def routes_for_operators_notin_shapes(
    merged_trips_routes: dg.GeoDataFrame) -> gpd.GeoDataFrame:
    missing_shapes = (merged_trips_routes[
        (merged_trips_routes._merge=="left_only") &
        (merged_trips_routes.calitp_itp_id != 323)] # gtfs_utils fixes Metrolink upstream 
      [["calitp_itp_id", "route_id", "trip_id", "shape_id"]] 
      .reset_index(drop=True)
     )
    
    MISSING_ITP_IDS = list(missing_shapes.calitp_itp_id.unique())
    MISSING_TRIPS = list(missing_shapes.trip_id.unique())
    
    # Only grab trip info for the shape_ids that are missing, or, appear in missing_shapes
    keep_trip_cols = ["calitp_itp_id", "route_id", "shape_id",
                      "trip_id", "trip_key"]

    missing_trips = gtfs_utils.get_trips(
        selected_date = prep_data.ANALYSIS_DATE,
        itp_id_list = MISSING_ITP_IDS,
        trip_cols = keep_trip_cols,
        get_df = True,
        custom_filtering = {"trip_id": MISSING_TRIPS}
    )
    
    # Since there are multiple trips, we'll sort the same way, and keep the first one
    # since we care about showing route-level geom
    group_cols = ["calitp_itp_id", "shape_id", "route_id"]
    missing_trips2 = (missing_trips.sort_values(group_cols + ["trip_id"])
                      .drop_duplicates(subset=group_cols)
                      .reset_index(drop=True)
    )
    
    stop_info_trips = (
        tbl.views.gtfs_schedule_dim_stop_times()
        >> filter(_.calitp_itp_id.isin(MISSING_ITP_IDS))
        >> filter(_.trip_id.isin(MISSING_TRIPS))
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
    print("Drop duplicates in stop_info_trips")

    # Assemble line geometry
    missing_routes = geography_utils.make_routes_line_geom_for_missing_shapes(stop_info_trips)
    
    # Merge route_id back in, which is lost when it 
    # passes through make_routes_line_geom_for_missing_shapes
    # Also, get rid of calitp_url_number
    missing_routes2 = pd.merge(
        missing_routes.drop(columns="calitp_url_number"),
        stop_info_trips[["calitp_itp_id", "shape_id", "route_id"]].drop_duplicates(),
        on = ["calitp_itp_id", "shape_id"],
        how = "inner",
        validate = "1:m",
    )
    
    return missing_routes2    


# Assemble routes file
def make_routes_shapefile():
    time0 = datetime.now()
    
    # Read in local parquets
    trips = dd.read_parquet(
        f"{prep_data.GCS_FILE_PATH}trips.parquet")
    routes = dg.read_parquet(
        f"{prep_data.GCS_FILE_PATH}routelines.parquet")

    df = merge_trips_to_routes(trips, routes)
    
    time1 = datetime.now()
    print(f"Read in data and merge shapes to routes: {time1-time0}")    
    
    routes_not_in_shapes = routes_for_operators_notin_shapes(df)
    
    if len(routes_not_in_shapes) > 0:
        routes_assembled = (dd.multi.concat([
            df[df._merge=="both"].drop(columns = "_merge"), 
            routes_not_in_shapes], axis=0)
                        .compute()
                        .sort_values(["calitp_itp_id", "route_id"])
                        .drop_duplicates()
                        .reset_index(drop=True)
                       )
    else:
        routes_assembled = df[df._merge=="both"].drop(columns="_merge").reset_index(drop=True)
    
    time2 = datetime.now()
    print(f"Add in routes for operator-routes not in shapes.txt: {time2-time1}")
    
    
    # Attach agency_name
    agency_names = portfolio_utils.add_agency_name(
        SELECTED_DATE = prep_data.ANALYSIS_DATE)
    
    
    routes_assembled2 = pd.merge(
        routes_assembled,
        agency_names,
        on = "calitp_itp_id",
        how = "left",
        validate = "m:1"
    )
    
    latest_itp_id = portfolio_utils.latest_itp_id()
    
    routes_assembled2 = (pd.merge(
        routes_assembled2, 
        latest_itp_id,
        on = "calitp_itp_id",
        how = "inner",
        validate = "m:1")
     # Any renaming to be done before exporting
     .rename(columns = prep_data.RENAME_COLS)
     .sort_values(["itp_id", "route_id"])
     .reset_index(drop=True)
    )
    
    time3 = datetime.now()
    print(f"Routes script total execution time: {time3-time0}")

    return routes_assembled2
