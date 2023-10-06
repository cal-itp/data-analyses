"""
Pre-processing vehicle positions.
Drop all RT trips with less than 10 min of data.
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


def get_vp_direction(vp_gddf) -> dd.DataFrame:
    """
    """
    vp_gddf = vp_gddf.assign(
        prior_vp_idx = vp_gddf.vp_idx - 1
    )
    # Dask gdf does not like to to renaming on-the-fly
    vp_gddf_renamed = (vp_gddf[["vp_idx", "geometry"]]
                       .add_prefix("prior_")
                       .set_geometry("prior_geometry")
                      )
    
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
        #meta = ("vp_primary_direction", "object")
    )
    
    results = vp_gddf_with_prior[["vp_idx", "vp_primary_direction"]]
        
    return results


def attach_prior_vp_add_direction(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    """
    t0 = datetime.datetime.now()
    INPUT_FILE = dict_inputs["stage1"]

    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage",
        columns = ["trip_instance_key", "vp_idx", "x", "y"],
    )
    print(f"number of partitions on import: {vp.npartitions}")
        
    usable_bounds = segment_calcs.get_usable_vp_bounds_by_trip(vp)
    
    vp2 = dd.merge(
        vp,
        usable_bounds,
        on = "trip_instance_key",
        how = "inner"
    )
    
    vp_gddf = dg.from_dask_dataframe(
        vp2,
        geometry = dg.points_from_xy(vp, x="x", y="y", crs=WGS84)
    ).set_crs(WGS84).to_crs(PROJECT_CRS).drop(columns = ["x", "y"]).persist()
    
    t1 = datetime.datetime.now()
    print(f"persist vp gddf: {t1 - t0}")
        
    t2 = datetime.datetime.now()
    
    vp_direction = vp_gddf.map_partitions(
        get_vp_direction, 
        meta = {"vp_idx": "int64",
                "vp_primary_direction": "object"},
        align_dataframes = False
    )
    
    t3 = datetime.datetime.now()
    print(f"get direction: {t3-t2}")
    '''
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage",
    )
    usable_vp_with_dir = pd.merge(
        df,
        vp_direction.compute(),
        on = "vp_idx",
        how = "left"
    )

    usable_vp_with_dir = usable_vp_with_dir.assign(
        vp_primary_direction = usable_vp_with_dir.vp_primary_direction.fillna("Unknown"),
    )    
    
    t4 = datetime.datetime.now()
    
    print(f"merge back onto vp: {t4-t3}")
    print(usable_vp_with_dir.dtypes)
    '''
    # Either use dask (which kills kernel here) or remove the existing folder of output
    # https://stackoverflow.com/questions/69092126/is-it-possible-to-change-the-output-filenames-when-saving-as-partitioned-parquet
    export_path = f"{SEGMENT_GCS}vp_direction_{analysis_date}"
    
    vp_direction = vp_direction.repartition(npartitions=5)
    
    vp_direction.to_parquet(
        export_path,
        # partition_cols is for pd.to_parquet, partition_on is for dd.to_parquet
        # if we don't delete the entire folder of partitioned parquets, this
        # can delete it if the partitions have the same name
        #existing_data_behavior = "delete_matching" 
    )

    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/usable_rt_vp_test.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    attach_prior_vp_add_direction(analysis_date, STOP_SEG_DICT)

    end = datetime.datetime.now()
    logger.info(f"attach direction: {end - start}")
