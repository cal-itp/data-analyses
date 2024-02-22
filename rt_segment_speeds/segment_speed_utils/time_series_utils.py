"""
Functions for creating time-series data
by concatenating aggregated data across multiple months.
"""
import datetime
import geopandas as gpd
import gcsfs
import pandas as pd

from dask import delayed, compute
from pathlib import Path
from typing import Literal

from segment_speed_utils.project_vars import SEGMENT_GCS

fs = gcsfs.GCSFileSystem()

OPERATOR_COLS = ["schedule_gtfs_dataset_key", "name",
                 "organization_source_record_id", "organization_name",
                 "base64_url", "caltrans_district"]
STOP_PAIR_COLS = ["stop_pair"] 
ROUTE_DIR_COLS = ["route_id", "direction_id"]


def concatenate_datasets_across_months(
    dataset_name: Literal["speeds_route_dir_segments", "speeds_route_dir"]
) -> pd.DataFrame:
    """
    Concatenate parquets across all months of available data.
    """
    list_of_files = fs.glob(f"{SEGMENT_GCS}{dataset_name}_*")
    
    # If the dataset name includes a folder, parse that away
    dataset_stem = Path(dataset).stem

    dates = [Path(i).stem.split(f"{dataset_stem}_")[1] for i in list_of_files]
    
    dfs = [
        delayed(gpd.read_parquet)(
            f"{SEGMENT_GCS}{dataset_name}_{d}.parquet"
        ).assign(
            service_date = pd.to_datetime(d)
        ) for d in dates
    ]
    
    df = delayed(pd.concat)(
        dfs, axis=0, ignore_index=True
    ).sort_values(
        ["organization_name", "service_date"] + ROUTE_DIR_COLS
    ).reset_index(drop=True)    
    
    df = compute(df)[0]    
    
    return df