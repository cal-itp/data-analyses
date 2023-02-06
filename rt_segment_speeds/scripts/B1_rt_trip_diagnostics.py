"""
Trip-level diagnostics.

Use this to decide whether the trip is a 
good or bad batch.

Get aggregations, like time elapsed for trip, 
how many vehicle positions are present, etc.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import pandas as pd
import gcsfs
import geopandas as gpd
import sys

from dask import delayed
from loguru import logger

import dask_utils
from update_vars import SEGMENT_GCS, analysis_date

fs = gcsfs.GCSFileSystem()


def get_trip_stats(ddf: dd.DataFrame) -> pd.DataFrame:
    """
    Calculate trip-level RT stats, 
    such as minimum / maximum vehicle_timestamp and 
    number of segments it has vehicle positions in.
    """
    trip_cols = ["gtfs_dataset_key", "_gtfs_dataset_name", 
                 "trip_id", "route_dir_identifier"]    
    timestamp_col = "location_timestamp"
    
    min_time = (ddf.groupby(trip_cols)
            [timestamp_col].min()
            .reset_index()
            .rename(columns = {timestamp_col: "trip_start"})
           )

    max_time = (ddf.groupby(trip_cols)
                [timestamp_col].max()
                .reset_index()
                .rename(columns = {timestamp_col: "trip_end"})
               )
    
    segments_with_vp = (ddf.groupby(trip_cols)
                    .segment_sequence
                    .nunique().reset_index()
                    .rename(columns = {
                        "segment_sequence": "num_segments_with_vp"})
                   )
    
    trip_stats = dd.merge(
        min_time,
        max_time,
        on = trip_cols,
        how = "outer"
    ).merge(
        segments_with_vp,
        on = trip_cols,
        how = "left"
    )
    
    return trip_stats


    
if __name__ == "__main__":

    logger.add("../logs/B1_rt_trip_diagnostics.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    all_files = fs.ls(f"{SEGMENT_GCS}vp_sjoin/")
    
    vp_seg_files = [f"gs://{i}" for i in all_files if 'vp_segment' in i 
                    and analysis_date in i
                   ]
    
    dfs = [delayed(pd.read_parquet)(f) for f in vp_seg_files]
    ddf = dd.from_delayed(dfs) 
    
    # Try map_partitions here
    trip_stats = ddf.map_partitions(get_trip_stats, 
                   meta= {
                       "gtfs_dataset_key": "object",
                       "_gtfs_dataset_name": "object",
                       "trip_id": "object",
                       "route_dir_identifier": "int64",
                       "trip_start": "datetime64[ns]",
                       "trip_end": "datetime64[ns]",
                       "num_segments_with_vp": "int64",
                   })   
    
    trip_stats.compute().to_parquet(
        f"{SEGMENT_GCS}trip_diagnostics_{analysis_date}.parquet")

    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")