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
    
    # Attach agency_name
    agency_names = portfolio_utils.add_agency_name(
        SELECTED_DATE = prep_data.ANALYSIS_DATE)
    
    routes_with_names = pd.merge(
        df,
        agency_names,
        on = "calitp_itp_id",
        how = "left",
        validate = "m:1"
    )
    
    latest_itp_id = portfolio_utils.latest_itp_id()
    
    routes_with_names = (pd.merge(
        routes_with_names, 
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
