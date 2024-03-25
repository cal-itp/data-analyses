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

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS

fs = gcsfs.GCSFileSystem()

OPERATOR_COLS = ["schedule_gtfs_dataset_key", "name",
                 "organization_source_record_id", "organization_name",
                 "base64_url", "caltrans_district"]
STOP_PAIR_COLS = ["stop_pair"] 
ROUTE_DIR_COLS = ["route_id", "direction_id"]


def concatenate_datasets_across_dates(
    gcs_bucket: str,
    dataset_name: Literal["speeds_route_dir_segments", "speeds_route_dir"],
    date_list: list,
    data_type: Literal["df", "gdf"] = "gdf",
    get_pandas: bool = True,
    **kwargs
) -> pd.DataFrame:
    """
    Concatenate parquets across all months of available data.
    """  
    if data_type == "gdf":
        dfs = [
            delayed(gpd.read_parquet)(
                f"{gcs_bucket}{dataset_name}_{d}.parquet",
                **kwargs
            ).assign(
                service_date = pd.to_datetime(d)
            ) for d in date_list
        ]
    else:
        dfs = [
            delayed(pd.read_parquet)(
                f"{gcs_bucket}{dataset_name}_{d}.parquet",
                **kwargs
            ).assign(
                service_date = pd.to_datetime(d)
            ) for d in date_list
        ]
    
    df = delayed(pd.concat)(
        dfs, axis=0, ignore_index=True
    ) 
    
    if get_pandas:
        df = compute(df)[0]    
    
    return df