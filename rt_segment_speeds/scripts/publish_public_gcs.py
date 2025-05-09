"""
Publish certain speeds files to public GCS
"""
import datetime
import geopandas as gpd
import pandas as pd

from pathlib import Path

from calitp_data_analysis import utils
from shared_utils import rt_dates, gtfs_utils_v2
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS, PUBLIC_GCS

if __name__ == "__main__":
    
    analysis_date = rt_dates.DATES["jul2024"]
    
    datasets = [
        GTFS_DATA_DICT.speedmap_segments.route_dir_single_segment,
    ]
    
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    for d in datasets:
        
        start = datetime.datetime.now()

        df = gpd.read_parquet(
            f"{SEGMENT_GCS}{d}_{analysis_date}.parquet",
            filters = [[("schedule_gtfs_dataset_key", "in", public_feeds)]]
        )
                
        utils.geoparquet_gcs_export(
            df,
            f"{PUBLIC_GCS}open_data/",
            f"{Path(d).stem}_{analysis_date}"
        )
        
        utils.geojson_gcs_export(
            df,
            f"{PUBLIC_GCS}open_data/",
            f"{Path(d).stem}_{analysis_date}",
            geojson_type = "geojson"
        )
            
        end = datetime.datetime.now()
        print(f"save {d} to public GCS: {end - start}")
