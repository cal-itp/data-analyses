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
from segment_speed_utils import vp_transform
from shared_utils import publish_utils, rt_utils 
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS

import google.auth
credentials, _ = google.auth.default()

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
        storage_options={"token": credentials.token}
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
    analysis_date: str,
    dict_inputs: dict = {}
):
    """
    """
    time0 = datetime.datetime.now()

    USABLE_VP = dict_inputs.speeds_tables.usable_vp

    vp_gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}_stage.parquet",
        columns = ["trip_instance_key", "vp_idx", "geometry"],
        storage_options={"token": credentials.token}
    ).to_crs(PROJECT_CRS)
    
    vp_condensed = vp_transform.condense_point_geom_to_line(
        vp_gdf,
        group_cols = ["trip_instance_key"],
        #sort_cols = ["trip_instance_key", "vp_idx"], not used?
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
    )[keep_cols].explode(column=keep_cols)
    
    vp_condensed.to_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet"
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"export vp direction: {time1 - time1}")
    
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

        # Add in direction of travel
        #get_vp_direction_column(
        #    analysis_date, 
        #    GTFS_DATA_DICT
        #)
    
        merge_in_vp_direction(
            analysis_date,
            GTFS_DATA_DICT
        )
        
        end = datetime.datetime.now()
        logger.info(
            f"{analysis_date}: add vp direction execution time: "
            f"{end - start}"
        )
        