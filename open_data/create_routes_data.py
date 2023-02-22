"""
Create routes file with identifiers including
route_id, route_name, operator name.
"""
import geopandas as gpd
import pandas as pd

from datetime import datetime

import prep_traffic_ops
from shared_utils import utils, portfolio_utils
from update_vars import analysis_date, TRAFFIC_OPS_GCS

def create_routes_file_for_export(analysis_date: str) -> gpd.GeoDataFrame:
    
    # Read in local parquets
    trips = prep_traffic_ops.import_trips(analysis_date)
    shapes = prep_traffic_ops.import_shapes(analysis_date)

    shape_cols = ["feed_key", "shape_id"]
    
    df = pd.merge(
        shapes,
        trips,
        on = shape_cols, 
        # if we use shape_array_key, metrolink doesn't have anything
        how = "inner"
    )
    
    drop_cols = ["route_short_name", "route_long_name", 
                 "route_desc", "feed_key", "trip_id"
                ]
    
    routes_assembled = (portfolio_utils.add_route_name(df)
                        .drop(columns = drop_cols)
                        .sort_values(["name", "route_id"])
                       .drop_duplicates(subset=[
                           "name", "route_id", "shape_id"])
                       .reset_index(drop=True)
                      )
    
    
    # Change column order
    route_cols = ['agency', 'route_id', 'route_type']
    agency_ids = ['base64_url', 'uri', 'feed_url']

    col_order = route_cols + ['route_name', 
        'shape_id', 'n_trips'
    ] + agency_ids + ['geometry']
    
    routes_assembled2 = (
        prep_traffic_ops.standardize_operator_info_for_exports(
            routes_assembled)
        [col_order]
        .reindex(columns = col_order)   
    )
    
    return routes_assembled2


if __name__ == "__main__":
    time0 = datetime.now()
    
    # Make an operator-feed level file (this is published)    
    # This is feed-level already, but we already keep only 1 feed per operator
    routes = create_routes_file_for_export(analysis_date)  
    
    utils.geoparquet_gcs_export(
        routes, 
        TRAFFIC_OPS_GCS, 
        "ca_transit_routes"
    )
    
    prep_traffic_ops.export_to_subfolder(
        "ca_transit_routes", analysis_date)
    
    time1 = datetime.now()
    print(f"Execution time for routes script: {time1-time0}")
