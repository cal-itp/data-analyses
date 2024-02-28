"""
Grab all files in the rollup
"""
import datetime
import geopandas as gpd
import pandas as pd

from pathlib import Path

from calitp_data_analysis import utils
from segment_speed_utils import helpers, time_series_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, PUBLIC_GCS
from shared_utils import rt_dates

if __name__ == "__main__":

    from segment_speed_utils.project_vars import CONFIG_PATH
    
    analysis_date_list = rt_dates.y2023_dates + rt_dates.y2024_dates
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    DATASETS = [
        STOP_SEG_DICT["route_dir_single_segment"], 
        STOP_SEG_DICT["route_dir_single_summary"]
    ]
    
    for d in DATASETS:
        
        start = datetime.datetime.now()

        df = time_series_utils.concatenate_datasets_across_months(
            d, analysis_date_list
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
