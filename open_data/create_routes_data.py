"""
Create routes file with identifiers including
route_id, route_name, operator name.
"""
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
        get_pandas = True
    )
    
    df = gtfs_schedule_wrangling.merge_shapes_to_trips(
        shapes,
        trips,
        merge_cols = ["feed_key", "shape_id"]
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
                      )#.to_crs(geography_utils.WGS84)
    
    
    # Change column order
    route_cols = ['agency', 'route_id', 'route_type']
    agency_ids = ['base64_url', 'uri']

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
