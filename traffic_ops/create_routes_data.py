"""
Create routes file with identifiers including
route_id, route_name, operator name.
"""
import geopandas as gpd
import pandas as pd

from datetime import datetime

import prep_data
from shared_utils import utils, portfolio_utils


def create_routes_file_for_export(analysis_date: str) -> gpd.GeoDataFrame:
    
    # Read in local parquets
    trips = prep_data.import_trips(analysis_date)
    shapes = prep_data.import_shapes(analysis_date)

    shape_cols = ["feed_key", "shape_id"]
    
    df = pd.merge(
        shapes,
        trips,
        on = shape_cols, 
        # if we use shape_array_key, metrolink doesn't have anything
        how = "inner"
    )
    
    drop_cols = ["route_short_name", "route_long_name", 
                 "route_desc", "feed_key"
                ]
    
    routes_assembled = (portfolio_utils.add_route_name(df)
                       .drop(columns = drop_cols)
                       .sort_values(["name", "route_id"])
                       .drop_duplicates(subset=["name", 
                                                "route_id", "shape_id"])
                       .reset_index(drop=True)
                       .rename(columns = prep_data.RENAME_COLS)
                      )
    
    return routes_assembled


if __name__ == "__main__":
    time0 = datetime.now()
    
    # Make an operator-feed level file (this is published)    
    # This is feed-level already, but we already keep only 1 feed per operator
    routes = create_routes_file_for_export(
        prep_data.ANALYSIS_DATE)  
    
    utils.geoparquet_gcs_export(
        routes, 
        prep_data.TRAFFIC_OPS_GCS, 
        "ca_transit_routes"
    )
    
    prep_data.export_to_subfolder(
        "ca_transit_routes", prep_data.ANALYSIS_DATE)
    
    time1 = datetime.now()
    print(f"Execution time for routes script: {time1-time0}")
