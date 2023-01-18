"""
Create routes file with identifiers including
route_id, route_name, operator name.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from datetime import datetime

import prep_data
from shared_utils import utils, geography_utils, portfolio_utils


def import_trips(analysis_date: str) -> pd.DataFrame:
    keep_cols = ["feed_key", "name", 
                 "trip_id", 
                 "route_id", "shape_id", 
                 "route_long_name", "route_short_name", "route_desc"
                ]
    
    trips = pd.read_parquet(
        f"{prep_data.COMPILED_CACHED_GCS}"
        f"trips_{analysis_date}_all.parquet", 
        columns = keep_cols
    )
    
    return trips
    
    
def import_shapes(analysis_date: str) -> gpd.GeodataFrame:
    keep_cols = ["feed_key", "shape_id", "n_trips", "geometry"]
    
    shapes = gpd.read_parquet(
        f"{prep_data.COMPILED_CACHED_GCS}"
        f"routelines_{analysis_date}_all.parquet", 
        columns = keep_cols
    ).to_crs(geography_utils.WGS84)
    
    return shapes
    

def create_routes_file_for_export(analysis_date: str) -> gpd.GeoDataFrame:
    
    # Read in local parquets
    trips = import_trips(analysis_date)
    shapes = import_shapes(analysis_date)

    shape_cols = ["feed_key", "shape_id"]
    
    df = pd.merge(
        shapes,
        trips,
        on = shape_cols, 
        # if we use shape_array_key, metrolink doesn't have anything
        how = "inner"
    )
    
    drop_cols = ["route_short_name", "route_long_name", 
                 "route_desc", "service_date", 
                 "feed_key"
                ]

    df_with_route_names = (portfolio_utils.add_route_name(df)
                           .drop(columns = drop_cols)
                           .sort_values(["name", "route_id"])
                           .drop_duplicates(subset=["name", 
                                                    "route_id", "shape_id"])
                           .reset_index(drop=True)
                           .rename(columns = prep_data.RENAME_COLS)
                          )

    return df_with_route_names


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
