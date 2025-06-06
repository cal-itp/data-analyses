"""
Export speeds for open data portal.
"""
import geopandas as gpd
import pandas as pd

from pathlib import Path

from calitp_data_analysis import utils
from shared_utils import gtfs_utils_v2
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS
import google.auth
credentials, project = google.auth.default()

def stage_open_data_exports(analysis_date: str):
    """
    For the datasets we publish to Geoportal, 
    export them to a stable GCS URL so we can always 
    read it in open_data/catalog.yml.
    """
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    datasets = [
        GTFS_DATA_DICT.speedmap_segments.segment_timeofday,
        GTFS_DATA_DICT.rt_stop_times.route_dir_timeofday
    ]

    for d in datasets:
        gdf = gpd.read_parquet(
            f"{SEGMENT_GCS}{d}_{analysis_date}.parquet",
            filters = [[("schedule_gtfs_dataset_key", "in", public_feeds)]],
            storage_options = {"token": credentials.token}
        )
        
        utils.geoparquet_gcs_export(
            gdf,
            f"{SEGMENT_GCS}export/",
            f"{Path(d).stem}"
        )
    
    print(f"overwrite {datasets}")
    
    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list 
    
    for analysis_date in analysis_date_list:
                
        stage_open_data_exports(analysis_date)
