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

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"

analysis_date = "2022-10-12"
fs = gcsfs.GCSFileSystem()


def get_trip_stats(ddf: dd.DataFrame) -> pd.DataFrame:
    """
    Calculate trip-level RT stats, 
    such as minimum / maximum vehicle_timestamp and 
    number of segments it has vehicle positions in.
    """
    trip_cols = ["calitp_itp_id", "trip_id", "route_dir_identifier"]
        
    min_time = (ddf.groupby(trip_cols)
            .vehicle_timestamp.min()
            .reset_index()
            .rename(columns = {"vehicle_timestamp": "trip_start"})
           )

    max_time = (ddf.groupby(trip_cols)
                .vehicle_timestamp.max()
                .reset_index()
                .rename(columns = {"vehicle_timestamp": "trip_end"})
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

    logger.add("./logs/B1_rt_trip_diagnostics.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    all_files = fs.ls(f"{DASK_TEST}vp_sjoin/")
    
    vp_seg_files = [f"gs://{i}" for i in all_files if 'vp_segment' in i]
    
    dfs = [delayed(pd.read_parquet)(f) for f in vp_seg_files]
    ddf = dd.from_delayed(dfs) 
    
    # Try map_partitions here
    trip_stats = ddf.map_partitions(get_trip_stats, 
                   meta= {
                       "calitp_itp_id": "int64",
                       "trip_id": "object",
                       "route_dir_identifier": "int64",
                       "trip_start": "datetime64[ns]",
                       "trip_end": "datetime64[ns]",
                       "num_segments_with_vp": "int64",
                   })   
    
    trip_stats.compute().to_parquet(
        f"{DASK_TEST}trip_diagnostics_{analysis_date}.parquet")

    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")