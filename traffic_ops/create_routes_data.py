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
from shared_utils import utils, geography_utils, portfolio_utils
from bus_service_utils import gtfs_build


# List of cols to drop from trips table
# Didn't remove after switching to gtfs_utils, but these 
# are datetime and will get rejected in the zipped shapefile conversion anyway
remove_trip_cols = ["service_date", "calitp_extracted_at", "calitp_deleted_at"]


def merge_trips_to_routes(
    trips: dd.DataFrame, 
    routes: dg.GeoDataFrame, 
    group_cols: list = ["calitp_itp_id", "shape_id"]
) -> dg.GeoDataFrame:
    # Routes or trips can contain multiple calitp_url_numbers 
    # for same calitp_itp_id-shape_id. Drop these now
    # dask can only sort by 1 column!
    # when publishing to open data portal, we want it at operator level, not feed level
    if group_cols == ["calitp_itp_id", "shape_id"]:
        routes = (routes.sort_values("calitp_url_number")
              .drop_duplicates(subset=group_cols)
              .reset_index(drop=True)
        )
        
        trips = (trips.sort_values("calitp_url_number")
             .drop_duplicates(subset=["calitp_itp_id", "trip_id"])
             .reset_index(drop=True)
            .drop(columns = remove_trip_cols)
        )
        
    else:
        routes = (routes
          .drop_duplicates(subset=group_cols)
          .reset_index(drop=True)
        )
        trips = (trips.drop_duplicates(
            subset=["calitp_itp_id", "calitp_url_number", "trip_id"])
             .reset_index(drop=True)
            .drop(columns = remove_trip_cols)
        )

    
    # Left only means in trips, but shape_id not found in shapes.txt
    # right only means in routes, but no route that has that shape_id 
    # only 1% falls into right_only
    keep_cols = group_cols + ['route_id', 
        'route_type', 'geometry',
    ]
    
    m1 = gtfs_build.merge_routes_trips(
        routes, 
        trips, 
        group_cols,
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


def create_routes_file_for_export(analysis_date: str, 
                                  group_cols: list) -> gpd.GeoDataFrame:
    # Read in local parquets
    trips = dd.read_parquet(
        f"{prep_data.COMPILED_CACHED_GCS}trips_{analysis_date}_all.parquet")
    routes = dg.read_parquet(
        f"{prep_data.COMPILED_CACHED_GCS}routelines_{analysis_date}_all.parquet")

    df = merge_trips_to_routes(
        trips, 
        routes, 
        group_cols = group_cols
    )
    
    routes_with_names = (add_route_agency_name(df)
                 .sort_values(["itp_id", "route_id"])
                 .reset_index(drop=True)
                 .drop_duplicates()
                )
    
    return routes_with_names


if __name__ == "__main__":
    time0 = datetime.now()
    
    # Make an operator level file (this is published)
    operator_level_cols = ["calitp_itp_id", "shape_id"]
    
    routes = create_routes_file_for_export(
        prep_data.ANALYSIS_DATE, operator_level_cols)  
    
    utils.geoparquet_gcs_export(
        routes, 
        prep_data.TRAFFIC_OPS_GCS, 
        "ca_transit_routes"
    )
    
    prep_data.export_to_subfolder(
        "ca_transit_routes", prep_data.ANALYSIS_DATE)

    
    # Make a feed level file (not published externally, 
    # publish to GCS for internal use)
    feed_level_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"]
    
    feed_routes = create_routes_file_for_export(
        prep_data.ANALYSIS_DATE, feed_level_cols
    )
    
    utils.geoparquet_gcs_export(
        feed_routes, 
        prep_data.TRAFFIC_OPS_GCS, 
        "ca_transit_routes_feed"
    )
    
    prep_data.export_to_subfolder(
        "ca_transit_routes_feed", prep_data.ANALYSIS_DATE)
    
    time1 = datetime.now()
    print(f"Execution time for routes script: {time1-time0}")
