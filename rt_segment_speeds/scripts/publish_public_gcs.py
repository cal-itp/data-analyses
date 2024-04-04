"""
Grab all files in the rollup
"""
import datetime
import geopandas as gpd
import pandas as pd

from pathlib import Path

from calitp_data_analysis import utils
from segment_speed_utils import time_series_utils
from shared_utils import rt_dates
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS, PUBLIC_GCS

if __name__ == "__main__":
    
    analysis_date_list = rt_dates.y2023_dates + rt_dates.y2024_dates
    
    STOP_SEG_DICT = GTFS_DATA_DICT.stop_segments
    
    DATASETS = [
        STOP_SEG_DICT.route_dir_single_segment, 
        STOP_SEG_DICT.route_dir_single_summary
    ]
    
    for d in DATASETS:
        
        start = datetime.datetime.now()

        df = time_series_utils.concatenate_datasets_across_dates(
            SEGMENT_GCS,
            d, 
            analysis_date_list,
            data_type = "gdf",
            get_pandas = True
        )
        
        dataset_stem = Path(d).stem
        export_file = f"time_series/{dataset_stem}.parquet"
        
        if isinstance(df, pd.DataFrame):
            df.to_parquet(f"{SEGMENT_GCS}{export_file}")
        
        elif isinstance(df, gpd.GeoDataFrame):
            utils.geoparquet_gcs_export(
                df,
                SEGMENT_GCS,
                export_file
            )
            
        end = datetime.datetime.now()
        print(f"save {d} to public GCS: {end - start}")
