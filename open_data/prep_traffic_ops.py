"""
Import trips, shapes, stops, stop_times files
and get it ready for GTFS schedule routes / stops datasets.
"""
import geopandas as gpd
import pandas as pd

from shared_utils import utils, schedule_rt_utils
from update_vars import TRAFFIC_OPS_GCS, analysis_date

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
    

def standardize_operator_info_for_exports(
    df: pd.DataFrame, 
    analysis_date: str
) -> pd.DataFrame:
    
    crosswalk = schedule_rt_utils.sample_schedule_feed_key_to_organization_crosswalk(
        df, 
        analysis_date,
        quartet_data = "schedule", 
        dim_gtfs_dataset_cols = [
            "key", "regional_feed_type",
            "base64_url", "uri"],
        dim_organization_cols = [
            "source_record_id", "name", "caltrans_district"]
    )
    
    df2 = pd.merge(
        df,
        crosswalk,
        on = "feed_key",
        how = "inner",
        validate = "m:1"
    )
        
    return df2
    
    
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
    "organization_name": "agency",
    "organization_source_record_id": "org_id",
    "route_name_used": "route_name",
}