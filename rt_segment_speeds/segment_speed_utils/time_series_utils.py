"""
Functions for creating time-series data
by concatenating aggregated data across multiple months.
"""
import datetime
import geopandas as gpd
import pandas as pd

from pathlib import Path
from typing import Literal, Union

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SCHED_GCS, SEGMENT_GCS
from shared_utils import dask_utils


def concatenate_datasets_across_dates(
    gcs_bucket: str,
    dataset_name: str,
    date_list: list,
    data_type: Literal["df", "gdf"],
    get_pandas: bool = True,
    **kwargs
) -> pd.DataFrame:
    """
    Concatenate parquets across all months of available data.
    """  
    paths = [f"{gcs_bucket}{dataset_name}" for date in date_list]

    df = dask_utils.get_ddf(
        paths, 
        date_list, 
        data_type = data_type,
        get_pandas=get_pandas,
        **kwargs
    )  
    
    return df


def clean_standardized_route_names(
    df: pd.DataFrame, 
) -> pd.DataFrame:
    """
    Clean up route names for operators that need
    additional parsing. Just keep the route_long_name instead of
    combining it with route_short_name.
    
    TODO: do titlecase?
    this is finicky, because some are CC (which we don't want titlecase)    
    """
    df_need_cleaning = df.loc[df.name.isin(operators_only_route_long_name)]

    df_ok = df.loc[~df.name.isin(operators_only_route_long_name)]
    
    df_need_cleaning = df_need_cleaning.assign(
        recent_combined_name = df_need_cleaning.route_long_name
    )
    
    df2 = pd.concat([
        df_need_cleaning,
        df_ok
    ], axis=0, ignore_index=True)      

    return df2

def parse_route_combined_name(df):
    df = df.assign(
        recent_combined_name = df.recent_combined_name.str.replace("__", " ")
    ).drop(
        columns = ["route_id"]
    ).rename(
        columns = {
            "recent_route_id2": "route_id",
            "recent_combined_name": "route_combined_name"
        }
    )
    
    return df

operators_only_route_long_name = [
    "Antelope Valley Transit Authority Schedule",
    "Bay Area 511 ACE Schedule",
    "Bay Area 511 Caltrain Schedule",
    "Bay Area 511 Emery Go-Round Schedule",
    "Bay Area 511 Petaluma Schedule",
    "Beach Cities GMV Schedule",
    "Bear Schedule",
    "Commerce Schedule",
    "Elk Grove Schedule",
    "Humboldt Schedule",
    "LA DOT Schedule",
    "Lawndale Beat GMV Schedule",
    "Redding Schedule",
    "Redwood Coast Schedule",
    "Santa Maria Schedule",
    "StanRTA Schedule",
    "VCTC GMV Schedule",
    "Victor Valley GMV Schedule",
    "Visalia Schedule",
    "Yolobus Schedule",
]