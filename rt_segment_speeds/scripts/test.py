"""
Add direction to vp.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import gcsfs
import numpy as np
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers, segment_calcs
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS, CONFIG_PATH)
from shared_utils import rt_utils


def get_vp_direction(vp_gddf: dg.GeoDataFrame) -> dd.DataFrame:
    """
    Attach the prior vp's geometry, and find the cardinal direction
    the vp is traveling.
    """   
    vp_gddf = vp_gddf.assign(
        prior_vp_idx = vp_gddf.vp_idx - 1
    )
    
    # Dask gdf does not like to to renaming on-the-fly
    vp_gddf_renamed = (vp_gddf[["vp_idx", "geometry"]]
                       .add_prefix("prior_")
                       .set_geometry("prior_geometry")
                      )
    
    # Filter to the rows where prior_vp_idx is within same trip_instance_key
    vp_gddf_with_prior = dd.merge(
        vp_gddf,
        vp_gddf_renamed,
        on = "prior_vp_idx",
        how = "inner"
    ).query('prior_vp_idx >= min_vp_idx')
        
    vp_gddf_with_prior["vp_primary_direction"] = vp_gddf_with_prior.apply(
        lambda x: 
        rt_utils.primary_cardinal_direction(x.prior_geometry, x.geometry), 
        axis=1, 
    )
    
    results = vp_gddf_with_prior[["vp_idx", "vp_primary_direction"]]
        
    return results


def attach_prior_vp_add_direction(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    For each vp, attach the prior_vp, and use
    the 2 geometry columns to find the direction 
    the vp is traveling.
    Make use of map partitions. Since export takes awhile,
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
    ).set_crs(WGS84).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    vp_gddf = vp_gddf.persist()
    
    time1 = datetime.datetime.now()
    logger.info(f"persist vp gddf: {time1 - time0}")
            
    vp_direction = vp_gddf.map_partitions(
        get_vp_direction, 
        meta = {"vp_idx": "int64",
                "vp_primary_direction": "object"},
        align_dataframes = False
    )
    
    vp_direction = vp_direction.repartition(npartitions=2)
    
    time2 = datetime.datetime.now()
    logger.info(f"map partitions for direction: {time2 - time1}")
        
    vp_direction.to_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}",
        overwrite=True)
    
    time3 = datetime.datetime.now()
    logger.info(f"export vp direction: {time3 - time2}")

    
def merge_onto_usable_vp(analysis_date: str, dict_inputs: dict):
    """
    Import the usable vp that was staged and merge with the direction parquet
    """
    time0 = datetime.datetime.now()
    
    INPUT_FILE = dict_inputs["stage1"]
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage",
    )
    
    vp_direction_df = pd.read_parquet(
        f"{SEGMENT_GCS}vp_direction_{analysis_date}"
    )
    
    print(vp_direction_df.dtypes)
    
    df_with_dir = pd.merge(
        df,
        vp_direction_df,
        on = "vp_idx",
        how = "left"
    )

    df_with_dir = df_with_dir.assign(
        vp_primary_direction = df_with_dir.vp_primary_direction.fillna("Unknown"),
    )    
    
    df_with_dir.to_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}",
        partition_cols = "gtfs_dataset_key",
        overwrite = True
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"export usable vp with direction: {time1 - time0}")
    
    
    
    
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
    merge_onto_usable_vp(analysis_date, STOP_SEG_DICT)
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
