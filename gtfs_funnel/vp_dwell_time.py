"""
Add dwell time to vp
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS

def import_vp(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Import vehicle positions with a subset of columns
    we need to check whether bus is dwelling
    at a location.
    """
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns = ["trip_instance_key", "vp_idx", 
                   "x", "y", "location_timestamp_local"
                  ]
    ).pipe(wrangle_shapes.vp_as_gdf, WGS84)
        
    return vp


def group_vp_by_location(vp: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Group vp by location (use geometry column).
    If the min_vp_idx and max_vp_idx differ, it means the 
    bus is staying at the same location and 
    there are multiple timestamps (difference in time = dwell time).
    """
    group_cols = ["trip_instance_key", "geometry"]
    
    grouped_df = vp.groupby(group_cols, observed=True, group_keys=False)
    
    min_vp = grouped_df.vp_idx.min().reset_index().rename(
        columns = {"vp_idx": "min_vp_idx"})
    max_vp = grouped_df.vp_idx.max().reset_index().rename(
        columns = {"vp_idx": "max_vp_idx"})
    
    vp_by_location = pd.merge(
        min_vp,
        max_vp,
        on = group_cols,
        how = "inner"
    )
    
    # What happens if bus returns to the same stop, so timestamps jump?
    # We care about grouping all the consecutive ones, not non-consecutive jumps
    vp_jumps = vp_by_location.loc[
        vp_by_location.min_vp_idx != vp_by_location.max_vp_idx
    ]
    
    new_vp_jumps = handle_non_consecutive(vp_jumps)
                  
    vp_by_location = vp_by_location.assign(
        n_location_vp = (vp_by_location.max_vp_idx - vp_by_location.min_vp_idx) + 1
    )
    
    return vp_by_location
    

def add_dwell_time(
    vp: gpd.GeoDataFrame,
    grouped_vp: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Add dwell time (in seconds). Dwell time is calculated
    for this vp_location, which may not necessarily be a bus stop.
    """
    keep_cols = ["vp_idx", "location_timestamp_local"]
    
    df = pd.merge(
        grouped_vp,
        vp[keep_cols].add_prefix("min_"),
        on = "min_vp_idx",
        how = "inner"
    ).merge(
        vp[keep_cols].add_prefix("max_"),
        on = "max_vp_idx",
        how = "inner"
    )
    
    df = df.assign(
        dwell_time_sec = (
            df.max_location_timestamp_local - 
            df.min_location_timestamp_local).dt.total_seconds().astype("int"),
        moving_timestamp_local = (df.max_location_timestamp_local - 
            df.min_location_timestamp_local)
    )
    
    # Only keep subset of columns 
    # Keep min_vp_idx, drop max_vp_idx, and we'll use dwell time to bump
    # location_timestamp_local if we need to
    df2 = df[
        ["min_vp_idx", "dwell_time_sec", "n_location_vp"]
    ].rename(columns = {"min_vp_idx": "vp_idx"})
    
    return df2
    
if __name__ == "__main__":
    
    LOG_FILE = "./logs/vp_preprocessing.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    analysis_date = "2024-04-17"
    
    start = datetime.datetime.now()
    
    vp = delayed(import_vp)(analysis_date)
    vp_grouped = delayed(group_vp_by_location)(vp)
    
    vp_with_dwell = delayed(add_dwell_time)(vp, vp_grouped)
    
    vp_with_dwell = compute(vp_with_dwell)[0]
    
    time1 = datetime.datetime.now()
    logger.info(f"compute dwell df: {time1 - start}")
    
    orig_vp = pd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
    )
    
    results = pd.merge(
        orig_vp,
        vp_with_dwell,
        on = "vp_idx",
        how = "inner"
    )
    
    results.to_parquet(
        f"{SEGMENT_GCS}vp_usable_with_dwell_{analysis_date}",
        partition_cols = "gtfs_dataset_key",
        overwrite=True
    )
    
    end = datetime.datetime.now()
    logger.info(f"merge with original and export: {end - time1}")
    logger.info(f"vp with dwell time: {end - start}")