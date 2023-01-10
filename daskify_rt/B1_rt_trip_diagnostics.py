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

from dask import delayed, compute
from dask.delayed import Delayed # use this for type hint
from loguru import logger

from shared_utils import rt_utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"

analysis_date = "2022-10-12"
fs = gcsfs.GCSFileSystem()


def categorize_scheduled_trips_time_of_day(trips: pd.DataFrame):
    """
    Categorize scheduled trips to time-of-day category based
    on trip first departure.
    """
    trips = trips.assign(
        departure_hour = pd.to_datetime(
            trips.trip_first_departure_ts, unit="s").dt.hour, 
        )
    
    trips = trips.assign(
        time_of_day = trips.departure_hour.apply(
            rt_utils.categorize_time_of_day)
    )
    
    return trips



@delayed
def import_vehicle_positions_on_segments(path: str) -> dd.DataFrame:
    """
    Import vehicle positions spatially joined to segments.
    Each operator has a file.
    """
    return dd.read_parquet(path)


@delayed
def get_trip_stats(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Calculate trip-level RT stats, 
    such as minimum / maximum vehicle_timestamp and 
    number of segments it has vehicle positions in.
    """
    trip_cols = ["calitp_itp_id", "trip_id", "route_dir_identifier"]
    
    ddf = ddf.drop_duplicates(
        subset=trip_cols + ["vehicle_timestamp"])
    
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


def compute_and_export(delayed_df: Delayed, analysis_date: str):
    """
    Run compute() on delayed object, which returns a tuple,
    and extract the df, which is the first and only object.
    
    Export this as parquet to GCS.
    """
    time0 = datetime.datetime.now()
    
    df = compute(delayed_df)[0]
    itp_id = df.calitp_itp_id.unique().compute().iloc[0]
    
    df.compute().to_parquet(
        f"{DASK_TEST}trip_diagnostics/"
        f"trip_stats_{itp_id}_{analysis_date}.parquet")
    
    time1 = datetime.datetime.now()
    logger.info(f"exported {itp_id}: {time1 - time0}")

    
if __name__ == "__main__":

    logger.add("./logs/B1_rt_trip_diagnostics.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    all_files = fs.ls(f"{DASK_TEST}vp_sjoin/")
    
    vp_seg_files = [f"gs://{i}" for i in all_files 
                    if 'vp_segment' in i and 
                    'vp_segment_182_' not in i and 
                    'vp_segment_4_' not in i and
                   'vp_segment_282_' not in i
                   ]
    
    results = []
    
    for f in vp_seg_files:
        logger.info(f"start file: {f}")
        
        df = import_vehicle_positions_on_segments(f)
        trip_agg = get_trip_stats(df)#.persist()
        compute_and_export(trip_agg, analysis_date)
    
            
    #results_ddfs = [compute(i)[0] for i in results]
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")