"""
Create routes file with identifiers including
route_id, route_name, operator name.
"""
import os
os.environ['USE_PYGEOS'] = '0'

import geopandas as gpd
import pandas as pd

from datetime import datetime

import prep_traffic_ops
from shared_utils import utils, geography_utils, portfolio_utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from update_vars import analysis_date, TRAFFIC_OPS_GCS


def create_routes_file_for_export(analysis_date: str) -> gpd.GeoDataFrame:
    
    # Read in local parquets
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = prep_traffic_ops.keep_trip_cols,
        get_pandas = True
    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = prep_traffic_ops.keep_shape_cols,
        get_pandas = True,
        crs = geography_utils.WGS84
    )
    
    df = gtfs_schedule_wrangling.merge_shapes_to_trips(
        shapes,
        trips,
        merge_cols = ["feed_key", "shape_id"]
    )
    
    drop_cols = ["route_short_name", "route_long_name", 
                 "route_desc", "trip_id"
                ]
    
    routes_assembled = (portfolio_utils.add_route_name(df)
                        .drop(columns = drop_cols)
                        .sort_values(["name", "route_id", "shape_id"])
                        .drop_duplicates(subset=[
                           "name", "route_id", "shape_id"])
                        .reset_index(drop=True)
                      )    
    routes_assembled2 = prep_traffic_ops.standardize_operator_info_for_exports(
            routes_assembled, analysis_date)
            
    return routes_assembled2


def finalize_export_df(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Suppress certain columns used in our internal modeling for export.
    """
    # Change column order
    route_cols = [
        'organization_source_record_id', 'organization_name',
        'route_id', 'route_type', 'route_name_used']
    shape_cols = ['shape_id', 'n_trips']
    agency_ids = ['base64_url', 'uri']
    
    col_order = route_cols + shape_cols + agency_ids + ['geometry']
    df2 = (df[col_order]
           .reindex(columns = col_order)
           .rename(columns = prep_traffic_ops.RENAME_COLS)
    )
    
    return df2


if __name__ == "__main__":
    time0 = datetime.now()
    
    # Make an operator-feed level file (this is published)    
    # This is feed-level already, but we already keep only 1 feed per operator
    routes = create_routes_file_for_export(analysis_date)  
    
    routes2 = finalize_export_df(routes)
    
    utils.geoparquet_gcs_export(
        routes2, 
        TRAFFIC_OPS_GCS, 
        "ca_transit_routes"
    )
    
    prep_traffic_ops.export_to_subfolder(
        "ca_transit_routes", analysis_date)
    
    time1 = datetime.now()
    print(f"Execution time for routes script: {time1-time0}")
