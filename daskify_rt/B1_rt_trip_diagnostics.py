"""
Trip-level diagnostics.

Use this to decide whether the trip is a 
good or bad batch.

Get aggregations, like time elapsed for trip, 
how many vehicle positions are present, etc.

Use this to understand dask.delayed and persist
behaviors.
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


@delayed
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
            .compute()
           )

    max_time = (ddf.groupby(trip_cols)
                .vehicle_timestamp.max()
                .reset_index()
                .rename(columns = {"vehicle_timestamp": "trip_end"})
                .compute()
               )
    
    segments_with_vp = (ddf.groupby(trip_cols)
                    .segment_sequence
                    .nunique().reset_index()
                    .rename(columns = {
                        "segment_sequence": "num_segments_with_vp"})
                    .compute()
                   )
    
    trip_stats = pd.merge(
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
    
    results = []
    
    for f in vp_seg_files:
        logger.info(f"start file: {f}")
        
        df = delayed(pd.read_parquet)(f)
        trip_agg = get_trip_stats(df)
        results.append(trip_agg)
        
    time1 = datetime.datetime.now()
    logger.info(f"start compute and export of results")
    
    dask_utils.compute_and_export(
        results,
        gcs_folder = DASK_TEST,
        file_name = f"trip_diagnostics_{analysis_date}",
        export_single_parquet = True
    )
    
    time2 = datetime.datetime.now() 
    logger.info(f"computed and exported trip_diagnostics: {time2 - time1}")

    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")