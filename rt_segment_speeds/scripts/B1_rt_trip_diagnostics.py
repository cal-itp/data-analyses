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
import geopandas as gpd
import sys

from loguru import logger

from shared_utils import dask_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, analysis_date


def trip_time_elapsed_segments_touched(
    ddf: dd.DataFrame,
    timestamp_col: str
) -> pd.DataFrame:
    """
    Calculate trip-level RT stats, 
    such as minimum / maximum vehicle_timestamp and 
    number of segments it has vehicle positions in.
    """
    trip_cols = ["gtfs_dataset_key", "_gtfs_dataset_name", 
                 "trip_id", "route_dir_identifier"]    
    
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


def get_trip_stats(analysis_date: str,
                   dict_inputs: dict):
    """
    Import vehicle positions data, 
    get trip stats with map_partitions, and export.
    """
    VP_FILE = dict_inputs["stage2"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]
    EXPORT_FILE = dict_inputs["rt_trip_diagnostics"]
    
    ddf = helpers.import_vehicle_positions(
        gcs_folder = f"{SEGMENT_GCS}vp_sjoin/",
        file_name = f"{VP_FILE}_{analysis_date}/",
        file_type = "df",
        columns = ["gtfs_dataset_key", "_gtfs_dataset_name", 
                   "trip_id", "route_dir_identifier",
                   "segment_sequence",
                   "location_timestamp"],
        partitioned = True
    ).repartition(partition_size="85MB")
    
    # Try map_partitions here
    trip_stats = ddf.map_partitions(
        trip_time_elapsed_segments_touched,
        TIMESTAMP_COL,
        meta= {
           "gtfs_dataset_key": "object",
           "_gtfs_dataset_name": "object",
           "trip_id": "object",
           "route_dir_identifier": "int64",
           "trip_start": "datetime64[ns]",
           "trip_end": "datetime64[ns]",
           "num_segments_with_vp": "int64",
       }).compute()
    
    trip_stats.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet")
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/B1_rt_trip_diagnostics.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    ROUTE_SEG_DICT = helpers.get_parameters(
        "../scripts/config.yml",
        "route_segments"
    )
    
    get_trip_stats(analysis_date, ROUTE_SEG_DICT)

    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")