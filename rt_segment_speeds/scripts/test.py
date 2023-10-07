"""
Add direction to vp.

Doing this with geopandas gdfs will crash kernel (2 geom cols too much).
Doing this with dask_geopandas gddfs takes ~25 min.
Doing this with dask ddfs (x, y) coords takes ~7 min.
Doing this with dask ddfs  + np arrays takes ~4 min.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers, segment_calcs
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS, CONFIG_PATH)
from shared_utils import rt_utils

def attach_prior_vp_add_direction(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    For each vp, attach the prior_vp, and use
    the 2 geometry columns to find the direction 
    the vp is traveling.
    Since export takes awhile,
    save out a parquet and read it in to merge later.
    """
    time0 = datetime.datetime.now()
    INPUT_FILE = dict_inputs["stage1"]

    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage",
        columns = ["trip_instance_key", "vp_idx", "x", "y"],
    ).repartition(npartitions=20)
      
    usable_bounds = segment_calcs.get_usable_vp_bounds_by_trip(vp)
    
    vp2 = dd.merge(
        vp,
        usable_bounds,
        on = "trip_instance_key",
        how = "inner"
    )
    
    # Convert the x, y into gdf, project it, since direction has to be
    # calculated in projected CRS
    vp_gddf = dg.from_dask_dataframe(
        vp2,
        geometry = dg.points_from_xy(vp2, x="x", y="y", crs=WGS84)
    ).set_crs(WGS84).to_crs(PROJECT_CRS)
    
    vp_ddf = vp_gddf.assign(
        x = vp_gddf.geometry.x,
        y = vp_gddf.geometry.y,
        prior_vp_idx = vp_gddf.vp_idx - 1
    ).drop(columns = "geometry")
    
    # Dask gdf doesn't like to be renamed on-the-fly
    # Make 1 partition for faster merging
    vp_ddf_renamed = vp_ddf[
        ["vp_idx", "x", "y"]
    ].add_prefix("prior_").repartition(npartitions=1)
    
    # Merge on prior_vp_idx's geometry, and filter to those rows
    # whose prior_vp_idx is from the same trip_instance_key
    full_df = dd.merge(
        vp_ddf,
        vp_ddf_renamed,
        on = "prior_vp_idx",
        how = "inner"
    ).query('prior_vp_idx >= min_vp_idx')[
        ["vp_idx", "prior_x", "prior_y", "x", "y"]
    ].reset_index(drop=True)
    
    full_df = full_df.persist()
    
    time1 = datetime.datetime.now()
    logger.info(f"persist vp gddf: {time1 - time0}")
    
    def column_into_array(df: dd.DataFrame, col: str) -> np.ndarray:
        return df[col].compute().to_numpy()
        
    vp_indices = column_into_array(full_df, "vp_idx")
    prior_geom_x = column_into_array(full_df, "prior_x")
    prior_geom_y = column_into_array(full_df, "prior_y") 
    current_geom_x = column_into_array(full_df, "x")
    current_geom_y = column_into_array(full_df, "y")
    
    distance_east = current_geom_x - prior_geom_x
    distance_north = current_geom_y - prior_geom_y

    direction_result = np.vectorize(
        rt_utils.cardinal_definition_rules)(distance_east, distance_north)
    
    # Stack our results and convert to df
    results_array = np.column_stack((vp_indices, direction_result))
    
    vp_direction = pd.DataFrame(
        results_array, 
        columns = ["vp_idx", "vp_primary_direction"]
    ).astype({"vp_idx": "int64"})

    time2 = datetime.datetime.now()
    logger.info(f"np vectorize arrays for direction: {time2 - time1}")
    
    vp_direction.to_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet")    


def add_direction_to_usable_vp(analysis_date: str, dict_inputs: dict):
    """
    Merge staged vp_usable (partitioned by gtfs_dataset_key)
    to the vp direction results.
    """
    INPUT_FILE = dict_inputs["stage1"]
    
    usable_vp = pd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage"
    )
    
    vp_direction = pd.read_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet"
    )
    
    # Do a left merge so that rows (esp first vp for each trip) can be filled in
    # with Unknowns later
    vp_with_dir = pd.merge(
        usable_vp,
        vp_direction,
        on = "vp_idx",
        how = "left"
    )
    
    vp_with_dir = vp_with_dir.assign(
        vp_primary_direction = vp_with_dir.vp_primary_direction.fillna("Unknown"),
    )    
    
    vp_with_dir.to_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}",
        partition_cols = "gtfs_dataset_key",
    )


if __name__ == "__main__":
    
    LOG_FILE = "../logs/find_vp_direction.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    attach_prior_vp_add_direction(analysis_date, STOP_SEG_DICT)
    
    time1 = datetime.datetime.now()
    logger.info(f"export vp direction: {time1 - start}")
    
    add_direction_to_usable_vp(analysis_date, STOP_SEG_DICT)
        
    end = datetime.datetime.now()
    logger.info(f"export usable vp with direction: {end - time1}")
    logger.info(f"execution time: {end - start}")
