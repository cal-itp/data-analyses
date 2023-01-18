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
remove_trip_cols = ["service_date"]



def add_route_agency_name(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    
    route_name_used = portfolio_utils.add_route_name(
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
