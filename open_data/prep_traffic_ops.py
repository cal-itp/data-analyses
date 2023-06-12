"""
Import trips, shapes, stops, stop_times files
and get it ready for GTFS schedule routes / stops datasets.
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from shared_utils import (utils, rt_dates, rt_utils, 
                          geography_utils, portfolio_utils)

from update_vars import (TRAFFIC_OPS_GCS, 
                         COMPILED_CACHED_VIEWS, analysis_date)

keep_trip_cols = [
    "feed_key", "name", 
    "trip_id", 
    "route_id", "route_type", 
    "shape_id",
    "route_long_name", "route_short_name", "route_desc"
]

keep_shape_cols = [
    "feed_key", "shape_id",
    "n_trips", "geometry"
]
  
keep_stop_cols = [
    "feed_key",
    "stop_id", "stop_name", 
    "geometry"
] 

keep_stop_time_cols = [
    "feed_key", "trip_id", "stop_id"
]    
    

    
def standardize_operator_info_for_exports(df: pd.DataFrame) -> pd.DataFrame:
    
    # Add a decoded version for base64_url
    df2 = (portfolio_utils.add_agency_identifiers(df, analysis_date)
           .rename(columns = {
               "gtfs_dataset_key": "schedule_gtfs_dataset_key",
               "name": "schedule_gtfs_dataset_name"})
    )
    
    
    # Clean organization name
    df3 = portfolio_utils.get_organization_name(
        df2, 
        analysis_date, 
        merge_cols = ["schedule_gtfs_dataset_key", "schedule_gtfs_dataset_name"]
    ).rename(columns = RENAME_COLS)
    
    return df3
    
    
def export_to_subfolder(file_name: str, analysis_date: str):
    """
    We always overwrite the same geoparquets each month, and point our
    shared_utils/shared_data_catalog.yml to the latest file.
    
    But, save historical exports just in case.
    """
    file_name_sanitized = utils.sanitize_file_path(file_name)
    
    gdf = gpd.read_parquet(
        f"{TRAFFIC_OPS_GCS}{file_name_sanitized}.parquet")
        
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