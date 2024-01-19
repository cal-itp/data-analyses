"""
Condense vp into arrays by trip-direction.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import vp_transform, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS


def condense_vp_to_linestring(
    analysis_date: str, 
    dict_inputs: dict
):
    """
    Turn vp (df with point geometry) into a condensed 
    linestring version.
    We will group by trip and direction and save out 
    the vp point geom into a shapely.LineString.
    """
    USABLE_VP = dict_inputs["usable_vp_file"]
    EXPORT_FILE = dict_inputs["vp_condensed_line_file"]
    
    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}",
        columns = ["trip_instance_key", "x", "y", 
                    "vp_idx", "vp_primary_direction"],
    )
    
    vp_dtypes = vp.drop(columns = ["x", "y"]).dtypes.to_dict()

    vp_gdf = vp.map_partitions(
        wrangle_shapes.vp_as_gdf,
        crs = WGS84,
        meta = {
            **vp_dtypes,
            "geometry": "geometry"
        },
        align_dataframes = True
    )

    vp_condensed = vp_gdf.map_partitions(
        vp_transform.condense_point_geom_to_line,
        group_cols = ["trip_instance_key", "vp_primary_direction"],
        geom_col = "geometry",
        other_cols = ["vp_idx"],
        meta = {
            "trip_instance_key": "object",
            "vp_primary_direction": "object",
            "geometry": "geometry",
            "vp_idx": "object"
        },
        align_dataframes = False
    ).compute().set_geometry("geometry").set_crs(WGS84)
    
    utils.geoparquet_gcs_export(
        vp_condensed,
        f"{SEGMENT_GCS}condensed/",
        f"{EXPORT_FILE}_{analysis_date}"
    )
    
    return 


def prepare_vp_for_all_directions(analysis_date: str) -> gpd.GeoDataFrame:
    """
    """
    vp = delayed(gpd.read_parquet)(
        f"{SEGMENT_GCS}condensed/vp_condensed_{analysis_date}.parquet",
    )
  
    dfs = [
        delayed(vp_transform.combine_valid_vp_for_direction)(
            vp, direction) 
        for direction in wrangle_shapes.ALL_DIRECTIONS + ["Unknown"]
    ]
    
    results = [compute(i)[0] for i in dfs]
    gdf = pd.concat(
        results, axis=0, ignore_index=True
    ).sort_values(
        ["trip_instance_key", "vp_primary_direction"]
    ).reset_index(drop=True)
    
    utils.geoparquet_gcs_export(
        gdf,
        f"{SEGMENT_GCS}condensed/",
        f"vp_nearest_neighbor_{analysis_date}"
    )
   
    return 


if __name__ == "__main__":
    
    from update_vars import analysis_date_list, CONFIG_DICT
    
    LOG_FILE = "./logs/vp_preprocessing.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()

        #condense_vp_to_linestring(analysis_date, CONFIG_DICT)
        
        time1 = datetime.datetime.now()
        logger.info(
            f"{analysis_date}: condense vp for trip-direction "
            f"{time1 - start}"
        )
        
        prepare_vp_for_all_directions(analysis_date)
        
        end = datetime.datetime.now()
        logger.info(
            f"{analysis_date}: prepare vp to use in nearest neighbor: "
            f"{end - time1}"
        ) 
        logger.info(
            f"{analysis_date}: vp condenser script execution time: "
            f"{end - start}"
        )        