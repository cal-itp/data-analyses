"""
Pre-processing vehicle positions.
Drop all RT trips with less than 5 min of data.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

#from shared_utils import utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)

def trip_time_elapsed(
    ddf: dd.DataFrame, 
    group_cols: list,
    timestamp_col: str
):
    """
    Group by trip and calculate the time elapsed (max_time-min_time)
    for RT vp observed.
    """
    min_time = (ddf.groupby(group_cols)
                [timestamp_col]
                .min()
                .reset_index()
                .rename(columns = {timestamp_col: "min_time"})
               )
                 
    
    max_time = (ddf.groupby(group_cols)
                [timestamp_col]
                .max()
                .reset_index()
                .rename(columns = {timestamp_col: "max_time"})
               )
    
    df = dd.merge(
        min_time,
        max_time,
        on = group_cols,
        how = "outer"
    )
    
    df = df.assign(
        trip_time_sec = (df.max_time - df.min_time) / np.timedelta64(1, "s")
    )

    return df
    
    
def get_valid_trips_by_time_cutoff(
    ddf: dd.DataFrame, 
    timestamp_col: str,
    trip_time_min_cutoff: int
)-> pd.DataFrame:
    """
    Filter down trips by trip time elapsed.
    Set the number of minutes to do cut-off for at least x min of RT.
    """
    trip_cols = ["gtfs_dataset_key", "trip_id"]
    trip_stats = trip_time_elapsed(
        ddf,
        trip_cols,
        timestamp_col
    )
    
    usable_trips = (trip_stats[
        trip_stats.trip_time_sec >= trip_time_min_cutoff * 60]
                    [trip_cols]
                    .drop_duplicates()
                    .reset_index(drop=True)
                   )
    
    return usable_trips


def pare_down_vp_to_valid_trips(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    Pare down vehicle positions that have been joined to segments
    to keep the enter / exit timestamps.
    Also, exclude any bad batches of trips.
    """
    INPUT_FILE_PREFIX = dict_inputs["stage0"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]
    TIME_CUTOFF = dict_inputs["time_min_cutoff"]
    EXPORT_FILE = dict_inputs["stage1"]

    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE_PREFIX}_{analysis_date}.parquet"
    )
    
    usable_trips = get_valid_trips_by_time_cutoff(
        vp,
        TIMESTAMP_COL,
        TIME_CUTOFF
    )
    
    usable_vp = pd.merge(
        vp,
        usable_trips,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    ).sort_values(
        ["gtfs_dataset_key", "trip_id", 
         "location_timestamp_local"]
    ).drop_duplicates().reset_index(drop=True)
    
    # Let's convert to tabular now, make use of partitioning
    # We want to break up sjoins, so we can wrangle it to points on-the-fly
    usable_vp = usable_vp.assign(
        x = usable_vp.geometry.x,
        y = usable_vp.geometry.y,
        vp_idx = usable_vp.index.astype("int32")
    ).drop(columns = "geometry")
    
    usable_vp.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}",
        partition_cols = ["gtfs_dataset_key"]
    )

    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/usable_rt_vp.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    # Doesn't matter which dictionary to use
    # We're operating on same vp, and it's only in the next stage
    # that which segments used matters
    ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
   
    time1 = datetime.datetime.now()
    pare_down_vp_to_valid_trips(
        analysis_date,
        dict_inputs = ROUTE_SEG_DICT
    )
    
    logger.info(f"pare down vp")
   
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")