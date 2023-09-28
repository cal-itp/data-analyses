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
from calitp_data_analysis import utils, geography_utils
from shared_utils import portfolio_utils
from segment_speed_utils import helpers
from update_vars import analysis_date, TRAFFIC_OPS_GCS


def create_routes_file_for_export(date: str) -> gpd.GeoDataFrame:
    
    # Read in local parquets
    trips = helpers.import_scheduled_trips(
        date,
        columns = prep_traffic_ops.keep_trip_cols,
        get_pandas = True
    ).dropna(subset="shape_array_key")
    
    shapes = helpers.import_scheduled_shapes(
        date,
        columns = prep_traffic_ops.keep_shape_cols,
        get_pandas = True,
        crs = geography_utils.WGS84
    ).dropna(subset="shape_array_key")
    
    df = pd.merge(
        shapes,
        trips,
        on = "shape_array_key",
        how = "inner"
    ).drop(columns = "trip_id").drop_duplicates(subset="shape_array_key")
 
    df2 = remove_erroneous_shapes(df)    
        
    drop_cols = ["route_short_name", "route_long_name", "route_desc"]
    
    routes_assembled = (portfolio_utils.add_route_name(df2)
                        .drop(columns = drop_cols)
                        .sort_values(["name", "route_id", "shape_id"])
                        .drop_duplicates(subset=[
                           "name", "route_id", "shape_id"])
                        .reset_index(drop=True)
                      )    
    routes_assembled2 = prep_traffic_ops.standardize_operator_info_for_exports(
            routes_assembled, date)
            
    return routes_assembled2


def remove_erroneous_shapes(
    shapes_with_route_info: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Check if line is simple for Amtrak. If it is, keep. 
    If it's not simple (line crosses itself), drop.
    
    In Jun 2023, some Amtrak shapes appeared to be funky, 
    but in prior months, it's been ok.
    Checking for length is fairly time-consuming.
    """
    amtrak = "Amtrak Schedule"
    
    possible_error = shapes_with_route_info[shapes_with_route_info.name==amtrak]
    ok = shapes_with_route_info[shapes_with_route_info.name != amtrak]
    
    # Check if the line crosses itself
    ok_amtrak = possible_error.assign(
        simple = possible_error.geometry.is_simple
    ).query("simple == True").drop(columns = "simple")
    
    ok_shapes = pd.concat(
        [ok, ok_amtrak], 
        axis=0
    ).reset_index(drop=True)

    return ok_shapes


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
