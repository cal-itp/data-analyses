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
import gcsfs
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, PROJECT_CRS
from shared_utils import rt_utils

fs = gcsfs.GCSFileSystem()    

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
    INPUT_FILE = dict_inputs["usable_vp_file"]

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
    # calculated in projected CRS, so save out the projected CRS's x and y
    vp_gddf = dg.from_dask_dataframe(
        vp2,
        geometry = dg.points_from_xy(vp2, x="x", y="y")
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
        
    keep_cols = ["vp_idx", "prior_x", "prior_y", "x", "y"]
    full_df = full_df[keep_cols].compute()
    
    time1 = datetime.datetime.now()
    logger.info(f"persist vp gddf: {time1 - time0}")
    
    vp_indices = full_df.vp_idx.to_numpy()
    distance_east = full_df.x - full_df.prior_x
    distance_north = full_df.y - full_df.prior_y
    
    # Get the normalized direction vector split into x and y columns
    normalized_vector = wrangle_shapes.get_normalized_vector(
        (distance_east, distance_north)
    )

    # Stack our results and convert to df
    results_array = np.column_stack((
        vp_indices, 
        normalized_vector[0], 
        normalized_vector[1]
    ))
    
    vp_direction = pd.DataFrame(
        results_array, 
        columns = ["vp_idx", "vp_dir_xnorm", "vp_dir_ynorm"]
    ).astype({
        "vp_idx": "int64", 
        "vp_dir_xnorm": "float",
        "vp_dir_ynorm": "float"
    })
    
    # Get a readable direction (westbound, eastbound)
    vp_direction = vp_direction.assign(
        vp_primary_direction = vp_direction.apply(
            lambda x:
            rt_utils.cardinal_definition_rules(x.vp_dir_xnorm, x.vp_dir_ynorm), 
            axis=1
        )
    )

    time2 = datetime.datetime.now()
    logger.info(f"np vectorize arrays for direction: {time2 - time1}")
    
    vp_direction.to_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet")  
    
    del vp_direction, full_df, usable_bounds, vp, vp2

    return


def add_direction_to_usable_vp(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    Merge staged vp_usable (partitioned by gtfs_dataset_key)
    to the vp direction results.
    """
    INPUT_FILE = dict_inputs["usable_vp_file"]
    
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
    ).drop_duplicates(subset=["vp_idx", "vp_primary_direction"])   
    
    export_path = f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}"
    if fs.exists(export_path):
        fs.rm(export_path, recursive=True)
    
    vp_with_dir.to_parquet(
        export_path,
        partition_cols = "gtfs_dataset_key",
    )
    
    del usable_vp, vp_direction, vp_with_dir

    return


if __name__ == "__main__":
    
    from update_vars import analysis_date_list, CONFIG_DICT
    
    LOG_FILE = "./logs/find_vp_direction.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    for analysis_date in analysis_date_list:
    
        logger.info(f"Analysis date: {analysis_date}")

        start = datetime.datetime.now()

        attach_prior_vp_add_direction(analysis_date, CONFIG_DICT)

        time1 = datetime.datetime.now()
        logger.info(f"export vp direction: {time1 - start}")

        add_direction_to_usable_vp(analysis_date, CONFIG_DICT)

        end = datetime.datetime.now()
        logger.info(f"export usable vp with direction: {end - time1}")
        logger.info(f"execution time: {end - start}")