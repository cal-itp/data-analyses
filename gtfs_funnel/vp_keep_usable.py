"""
Pre-processing vehicle positions.
Drop all RT trips with less than 10 min of data.
Add direction to vp.

Doing this with geopandas gdfs will crash kernel (2 geom cols too much).
Doing this with dask_geopandas gddfs takes ~25 min.
Doing this with dask ddfs (x, y) coords takes ~7 min.
Doing this with dask ddfs  + np arrays takes ~4 min. (but persisting takes another 4 min)
Doing this with pandas and numpy arrays takes ~8 min.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS

import google.auth
credentials, _ = google.auth.default()

def find_valid_trips(
    vp: pd.DataFrame,
    timestamp_col: str,
    trip_time_min_cutoff: int
) -> list:
    """
    Group by trip and calculate the time elapsed (max_time-min_time)
    for RT vp observed.
    """
    min_time = vp.groupby("trip_instance_key")[timestamp_col].min()
    max_time = vp.groupby("trip_instance_key")[timestamp_col].max()
    
    # This is a pd.Series that calculates the trip time elapsed
    # in vp df
    trip_times = ((max_time - min_time) / np.timedelta64(1, "s"))
    
    # Subset it to trips that meet our minimum time cut-off
    # index is the trip_instance_key
    usable_trips = trip_times.loc[
        trip_times > (trip_time_min_cutoff * 60)
    ].index.tolist()
    
    return usable_trips


def pare_down_to_valid_trips(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    time0 = datetime.datetime.now()

    INPUT_FILE = dict_inputs.speeds_tables.raw_vp
    TIMESTAMP_COL = dict_inputs.speeds_tables.timestamp_col
    TIME_CUTOFF = dict_inputs.speeds_tables.time_min_cutoff
    EXPORT_FILE = dict_inputs.speeds_tables.usable_vp

    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
        storage_options={"token": credentials.token}
    ).to_crs(WGS84)
    
    usable_trips = find_valid_trips(vp, TIMESTAMP_COL, TIME_CUTOFF)
    
    vp = vp[vp.trip_instance_key.isin(usable_trips)].sort_values(
        ["gtfs_dataset_key", "trip_id", 
         TIMESTAMP_COL]
    ).drop_duplicates(
        subset=["trip_instance_key", TIMESTAMP_COL]
    ).reset_index(drop=True)

    
    vp = vp.assign(
        vp_idx = vp.index,
    )
    
    utils.geoparquet_gcs_export(
        vp,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}_stage"
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"pare down vp: {time1 - time0}")  
    
    return
 

if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    LOG_FILE = "./logs/vp_preprocessing.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()
  
        pare_down_to_valid_trips(
            analysis_date,
            GTFS_DATA_DICT
        )
        
        end = datetime.datetime.now()
        logger.info(
            f"{analysis_date}: pare down vp execution time: "
            f"{end - start}"
        )
        