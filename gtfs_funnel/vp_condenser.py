"""
Pre-processing vehicle positions.
Drop all RT trips with less than 10 min of data.

Condense vp into arrays by trip-direction.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import vp_transform
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS

def find_valid_trips(
    vp: pd.DataFrame,
    timestamp_cols: list,
    trip_time_min_cutoff: int
) -> list:
    """
    Group by trip and calculate the time elapsed (max_time-min_time)
    for RT vp observed.
    """
    min_time = vp.groupby("trip_instance_key")[timestamp_cols[0]].min()
    max_time = vp.groupby("trip_instance_key")[timestamp_cols[1]].max()
    
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
    """
    Drop trips that do not meet the minimum time cutoff.
    """
    time0 = datetime.datetime.now()
 
    INPUT_FILE = dict_inputs.speeds_tables.raw_vp2
    EXPORT_FILE = dict_inputs.speeds_tables.vp_dwell
    
    BOTH_TIMESTAMP_COLS = [*dict_inputs.speed_vars.timestamp_cols]
    TIMESTAMP_COL = dict_inputs.speeds_tables.timestamp_col
    TIME_CUTOFF = dict_inputs.speeds_tables.time_min_cutoff
    
    vp = delayed(gpd.read_parquet)(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
        columns = [
            "trip_instance_key", "n_vp", "vp_direction", "geometry"] +
            BOTH_TIMESTAMP_COLS,
    ).to_crs(WGS84)
    
    usable_trips = find_valid_trips(vp, BOTH_TIMESTAMP_COLS, TIME_CUTOFF).compute()
    
    vp = vp[vp.trip_instance_key.isin(usable_trips)].sort_values(
        ["trip_instance_key", TIMESTAMP_COL]
    ).reset_index(drop=True)
    
    vp = vp.assign(
        vp_idx = vp.index,
        x = vp.geometry.x,
        y = vp.geometry.y,
    ).drop(columns = "geometry")
    
    vp = compute(vp)[0]
    
    vp.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"pare down vp: {time1 - time0}")  
    
    return
    

def condense_vp_to_linestring(
    analysis_date: str, 
    dict_inputs: dict
):
    """
    Turn vp (df with point geometry) into a condensed 
    linestring version.
    We will group by trip and save out 
    the vp point geom into a shapely.LineString.
    """
    INPUT_FILE = dict_inputs.speeds_tables.vp_dwell
    EXPORT_FILE = dict_inputs.speeds_tables.vp_condensed_line
    
    BOTH_TIMESTAMP_COLS = [*dict_inputs.speed_vars.timestamp_cols]
    TIMESTAMP_COL = dict_inputs.speeds_tables.timestamp_col
    TIME_CUTOFF = dict_inputs.speeds_tables.time_min_cutoff
    
    vp = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
        columns = [
            "trip_instance_key", "n_vp", "vp_direction", "x", "y"] +
            BOTH_TIMESTAMP_COLS,
    ).pipe(
        geo_utils.vp_as_gdf, crs = WGS84
    )
    
    vp_condensed = delayed(vp_transform.condense_point_geom_to_line)(
        vp,
        group_cols = ["trip_instance_key"],
        geom_col = "geometry",
        array_cols = ["vp_idx", 
                      "location_timestamp_local", "moving_timestamp_local",
                      "vp_direction",
                     ],
        sort_cols = ["vp_idx"]
    ).set_geometry("geometry").set_crs(WGS84)
        
    vp_condensed = compute(vp_condensed)[0]
    
    utils.geoparquet_gcs_export(
        vp_condensed,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
        
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
        
        pare_down_to_valid_trips(analysis_date, GTFS_DATA_DICT)
        
        #condense_vp_to_linestring(analysis_date, GTFS_DATA_DICT)
        
        end = datetime.datetime.now()
        
        #logger.info(
        #    f"{analysis_date}: condense vp for trip "
        #    f"{end - start}"
        #)    