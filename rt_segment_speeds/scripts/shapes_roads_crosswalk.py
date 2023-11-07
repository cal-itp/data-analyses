"""
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              CONFIG_PATH, PROJECT_CRS)


def sjoin_shapes_to_roads(
    road_segments: gpd.GeoDataFrame,
    shapes: gpd.GeoDataFrame
) -> pd.DataFrame:
    
    keep_cols = ["shape_array_key", "linearid", 
       "mtfcc", "segment_sequence"]
    
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(25)
    )
    
    shapes_to_roads = gpd.sjoin(
        shapes,
        road_segments,
        how = "inner",
        predicate = "intersects"
    )[keep_cols].drop_duplicates()
    
    return shapes_to_roads

    
def main(analysis_date: str, dict_inputs: dict):
    
    start = datetime.datetime.now()
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        get_pandas = True,
        crs = PROJECT_CRS
    ).pipe(
        helpers.remove_shapes_outside_ca
    ).drop(columns = "index_right")
    
    keep_road_cols = ["linearid", "mtfcc", "segment_sequence"]
    
    road_segments = dg.read_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}",
        columns = keep_road_cols + ["geometry"]
    ).repartition(npartitions=5)
    
    keep_shape_cols = ["shape_array_key"]
    
    shape_cols_dtypes = shapes[keep_shape_cols].dtypes.to_dict()
    road_cols_dtypes = road_segments[keep_road_cols].dtypes.to_dict()
    
    sjoin_results = road_segments.map_partitions(
        sjoin_shapes_to_roads,
        shapes,
        meta = {
            **shape_cols_dtypes,
            **road_cols_dtypes,
        },
        align_dataframes = False
    )
    
    results = sjoin_results.compute()
    results.to_parquet(
        f"{SEGMENT_GCS}shape_road_crosswalk_{analysis_date}.parquet"
    )
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")


    
if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/sjoin_shapes_roads.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    ROAD_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "road_segments")

    for analysis_date in analysis_date_list:
        logger.info(f"Analysis date: {analysis_date}")
        main(analysis_date, ROAD_SEG_DICT)
    

