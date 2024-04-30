"""
Export the tables we used in the notebook
to create GTFS Digest to our public bucket.
"""
import geopandas as gpd
import pandas as pd

from pathlib import Path
from typing import Literal

from update_vars import GTFS_DATA_DICT 
from shared_utils import publish_utils
    
def grab_filepaths(
    table_section: Literal["digest_tables", "schedule_tables"], 
    file_keys: list) -> list:
    """
    https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/gtfs_analytics_data.yml
    
    table_section corresponds to "schedule_tables", "digest_tables", 
    "speeds_tables", etc
    """
    
    GCS = GTFS_DATA_DICT[table_section].dir
    
    filepaths = [GTFS_DATA_DICT[table_section][f] for f in file_keys]
    
    return [f"{GCS}{f}.parquet" for f in filepaths]
    

if __name__ == "__main__":
    
    digest_keys = [
        "route_schedule_vp", "route_segment_speeds", 
        "operator_profiles", "operator_routes_map", 
        "operator_sched_rt"
    ]  
    
    digest_filepaths = grab_filepaths("digest_tables", digest_keys)
    schedule_filepaths = grab_filepaths(
        "schedule_tables", ["monthly_scheduled_service"])
    
    for f in digest_filepaths + schedule_filepaths:
        publish_utils.write_to_public_gcs(
            f,
            f"{Path(f).name}"
        )