"""
Create routes file with identifiers including
route_id, route_name, agency_id, agency_name.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from datetime import datetime

import prep_data
from shared_utils import geography_utils, portfolio_utils
from bus_service_utils import gtfs_build

# List of cols to drop from trips table
# Didn't remove after switching to gtfs_utils, but these 
# are datetime and will get rejected in the zipped shapefile conversion anyway
remove_trip_cols = ["service_date", "calitp_extracted_at", "calitp_deleted_at"]


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
            .drop(columns = remove_trip_cols)
    )
    
    # Left only means in trips, but shape_id not found in shapes.txt
    # right only means in routes, but no route that has that shape_id 
    # only 1% falls into right_only
    keep_cols = [
        'calitp_itp_id', 'route_id', 'shape_id', 
        'route_type', 'geometry',
    ]
    
    
    m1 = gtfs_build.merge_routes_trips(
        routes, 
        trips, 
        shape_id_cols,
        crs = f"EPSG: {routes.crs.to_epsg()}"
    )
    
    m2 = (m1[m1._merge=="both"][keep_cols]
          .reset_index(drop=True) 
          .to_crs(geography_utils.WGS84)
          .drop_duplicates()
         )
        
    return m2


def add_route_agency_name(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    
    route_name_used = portfolio_utils.add_route_name(
        selected_date = prep_data.ANALYSIS_DATE,
    )
    
    route_cols_to_drop = ["route_short_name", "route_long_name", "route_desc"]
    for c in route_cols_to_drop:
        if c in df.columns:
            df = df.drop(columns = c)
    
    with_route_name = pd.merge(
        df, 
        route_name_used.drop_duplicates(subset=["calitp_itp_id", "route_id"]),
        on = ["calitp_itp_id", "route_id"],
        how = "inner",
        validate = "m:1"
    )
    
    # Attach agency_name
    agency_names = portfolio_utils.add_agency_name(
        selected_date = prep_data.ANALYSIS_DATE)
    
    with_agency_name = pd.merge(
        with_route_name,
        agency_names,
        on = "calitp_itp_id",
        how = "left",
        validate = "m:1"
    )
    
    # Only keep latest ITP IDS
    latest_itp_id = portfolio_utils.latest_itp_id()
    
    with_latest_id = (pd.merge(
        with_agency_name, 
        latest_itp_id,
        on = "calitp_itp_id",
        how = "inner",
        validate = "m:1")
        # Any renaming to be done before exporting
        .rename(columns = prep_data.RENAME_COLS)
    )
    
    return with_latest_id


# Assemble routes file
def make_routes_shapefile():
    time0 = datetime.now()
    
    # Read in local parquets
    trips = dd.read_parquet(
        f"{prep_data.COMPILED_CACHED_GCS}trips_{prep_data.ANALYSIS_DATE}_all.parquet")
    routes = dg.read_parquet(
        f"{prep_data.COMPILED_CACHED_GCS}routelines_{prep_data.ANALYSIS_DATE}_all.parquet")

    df = merge_trips_to_routes(trips, routes)
    
    time1 = datetime.now()
    print(f"Read in data and merge shapes to routes: {time1-time0}")        
    
    routes_with_names = (add_route_agency_name(df)
                         .sort_values(["itp_id", "route_id"])
                         .reset_index(drop=True)
                         .drop_duplicates()
                        )
    
    time3 = datetime.now()
    print(f"Routes script total execution time: {time3-time0}")

    return routes_with_names
