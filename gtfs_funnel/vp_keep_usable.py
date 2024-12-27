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
from segment_speed_utils.project_vars import PROJECT_CRS
from shared_utils import geo_utils, publish_utils, rt_utils
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS
import vp_transform

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
    
    # Add in direction of travel
    get_vp_direction_column(vp)
    
    time2 = datetime.datetime.now()
    logger.info(f"export vp direction: {time2 - time1}")
    
    return


def merge_in_vp_direction(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    Merge staged vp_usable with the vp direction results
    and export.
    """
    time0 = datetime.datetime.now()
    
    INPUT_FILE = dict_inputs.speeds_tables.usable_vp
    
    vp_direction = pd.read_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet"
    )
    
    # By the end of add_vp_direction, we return df, not gdf
    # Let's convert to tabular now, make use of partitioning    
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage.parquet",
    ).to_crs(WGS84).merge(
        vp_direction,
        on = "vp_idx",
        how = "inner"
    )
    
    vp = vp.assign(
        x = vp.geometry.x,
        y = vp.geometry.y
    ).drop(columns = "geometry")
    
    export_path = f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}"

    publish_utils.if_exists_then_delete(export_path)
    
    vp.to_parquet(
        export_path,
        partition_cols = "gtfs_dataset_key",
        # if we don't delete the entire folder of partitioned parquets, this
        # can delete it if the partitions have the same name
        #existing_data_behavior = "delete_matching" 
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"{analysis_date}: export usable vp with direction: {time1 - time0}")
    
    return 
    
    
def get_vp_direction_column(
    vp_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    """    
    vp_gdf = vp_gdf[
        ["trip_instance_key", "vp_idx", "geometry"]
    ].to_crs(PROJECT_CRS)
    
    vp_condensed = vp_transform.condense_by_trip(
        vp_gdf,
        group_cols = ["trip_instance_key"],
        sort_cols = ["trip_instance_key", "vp_idx"], 
        array_cols = ["vp_idx", "geometry"]        
    )
    
    vp_direction_series = []
    
    for row in vp_condensed.itertuples():
        vp_geom = np.array(getattr(row, "geometry"))
        next_vp_geom = vp_geom[1:]
    
        vp_direction = np.array(
            ["Unknown"] + 
            [rt_utils.primary_cardinal_direction(prior_vp, current_vp)
            for prior_vp, current_vp in zip(vp_geom, next_vp_geom)
        ])
        
        vp_direction_series.append(vp_direction)
    
    keep_cols = ["vp_idx", "vp_primary_direction"]
    
    vp_condensed = vp_condensed.assign(
        vp_primary_direction = vp_direction_series
    )[keep_cols].pipe(
        vp_transform.explode_arrays, 
        array_cols = keep_cols
    )
    
    vp_condensed.to_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet"
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
  
        pare_down_to_valid_trips(
            analysis_date,
            GTFS_DATA_DICT
        )
        
        merge_in_vp_direction(
            analysis_date,
            GTFS_DATA_DICT
        )
        
        end = datetime.datetime.now()
        logger.info(
            f"{analysis_date}: pare down vp, add direction execution time: "
            f"{end - start}"
        )
        