"""
If we project all vp against shape geometry
take a look.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS,
                                              PROJECT_CRS, CONFIG_PATH)

def project_vp_to_shape(
    vp: dd.DataFrame, 
    shapes: gpd.GeoDataFrame
):
    """
    shapely.project vp point geom onto shape_geometry.
    """
    shapes = shapes.rename(columns = {"geometry": "shape_geometry"})
    
    vp_gdf = gpd.GeoDataFrame(
        vp,
        geometry = gpd.points_from_xy(vp.x, vp.y),
        crs = WGS84
    ).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    gdf = pd.merge(
        vp_gdf,
        shapes,
        on = "shape_array_key",
        how = "inner"
    )
    
    gdf = gdf.assign(
        shape_meters = gdf.shape_geometry.project(gdf.geometry)
    )
    
    vp_projected_result = gdf[["vp_idx", "shape_meters"]]
    
    return vp_projected_result


def project_usable_vp_one_day(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    start = datetime.datetime.now()
    
    USABLE_VP = f'{dict_inputs["stage1"]}_{analysis_date}'
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        get_pandas = True
    )

    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}",
        columns = ["trip_instance_key", "vp_idx", "x", "y"]
    ).merge(
        trips,
        on = "trip_instance_key",
        how = "inner"
    )

    subset_shapes = pd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}",
        columns = ["trip_instance_key"]
    ).drop_duplicates().merge(
        trips,
        on = "trip_instance_key",
        how = "inner"
    ).shape_array_key.unique().tolist()
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        filters = [[("shape_array_key", "in", subset_shapes)]],
        get_pandas = True,
        crs = PROJECT_CRS
    )
        
    results = vp.map_partitions(
        project_vp_to_shape,
        shapes,
        meta = {"vp_idx": "int64",
               "shape_meters": "float64"},
        align_dataframes = False
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"map partitions: {time1 - start}")

    df = results.compute()
    df.to_parquet(
        f"{SEGMENT_GCS}projection/vp_projected_{analysis_date}.parquet")

    end = datetime.datetime.now()
    logger.info(f"compute and export: {end - time1}")
    logger.info(f"execution time: {end - start}")
    
    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/shapely_project_vp.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
        
        
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    for analysis_date in analysis_date_list:
        logger.info(f"Analysis date: {analysis_date}")
        
        project_usable_vp_one_day(analysis_date, STOP_SEG_DICT)
     