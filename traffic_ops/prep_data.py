"""
Import trips, shapes, stops, stop_times files
and get it ready for GTFS schedule routes / stops datasets.
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from shared_utils import (utils, rt_dates, rt_utils, 
                          geography_utils, portfolio_utils)

ANALYSIS_DATE = rt_utils.format_date(rt_dates.DATES["jan2023"])

GCS = "gs://calitp-analytics-data/data-analyses/"
TRAFFIC_OPS_GCS = f"{GCS}traffic_ops/"
COMPILED_CACHED_GCS = f"{GCS}rt_delay/compiled_cached_views/"
DATA_PATH = "./data/"
    
def import_trips(analysis_date: str) -> pd.DataFrame:
    keep_cols = ["feed_key", "name", 
                 "trip_id", 
                 "route_id", "route_type", "shape_id", 
                 "route_long_name", "route_short_name", "route_desc"
                ]
    
    trips = pd.read_parquet(
        f"{COMPILED_CACHED_GCS}trips_{analysis_date}.parquet", 
        columns = keep_cols
    )
    
    # Clean organization name
    trips2 = portfolio_utils.clean_organization_name(trips)
    
    return trips2
    
    
def import_shapes(analysis_date: str) -> gpd.GeoDataFrame:
    keep_cols = ["feed_key", "shape_id", "n_trips", "geometry"]
    
    shapes = gpd.read_parquet(
        f"{COMPILED_CACHED_GCS}routelines_{analysis_date}.parquet", 
        columns = keep_cols
    ).to_crs(geography_utils.WGS84)
    
    return shapes
    

def import_stops(analysis_date: str) -> gpd.GeoDataFrame:
    # Instead of keeping route_type_0, route_type_1, etc
    # keep stops table long, instead of wide
    # attach route_id, route_type as before
    keep_cols = [
        "feed_key",
        "stop_id", "stop_name", 
        "geometry"
    ] 
    
    stops = gpd.read_parquet(
        f"{COMPILED_CACHED_GCS}stops_{analysis_date}.parquet",
        columns = keep_cols
    ).to_crs(geography_utils.WGS84)
    
    return stops
    
    
def import_stop_times(analysis_date: str) -> pd.DataFrame:
    keep_cols = ["feed_key", "trip_id", "stop_id"]
    
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED_GCS}st_{analysis_date}.parquet",
        columns = keep_cols
    ).drop_duplicates().reset_index(drop=True)
    
    return stop_times

    
def export_to_subfolder(file_name: str, analysis_date: str):
    """
    We always overwrite the same geoparquets each month, and point our
    shared_utils/shared_data_catalog.yml to the latest file.
    
    But, save historical exports just in case.
    """
    file_name_sanitized = file_name.replace('.parquet', '')
    
    gdf = gpd.read_parquet(f"{TRAFFIC_OPS_GCS}{file_name_sanitized}.parquet")
        
    utils.geoparquet_gcs_export(
        gdf, 
        f"{TRAFFIC_OPS_GCS}export/", 
        f"{file_name_sanitized}_{analysis_date}"
    )
        
        
# Define column names, must fit ESRI 10 character limits
RENAME_COLS = {
    "name": "agency",
    "route_name_used": "route_name",
}