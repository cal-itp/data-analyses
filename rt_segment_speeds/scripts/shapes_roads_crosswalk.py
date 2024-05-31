"""
Create crosswalk to show which shapes can sjoin
to which road segments.

We want to filter out vp when it's not traveling on 
a scheduled shape.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers
from update_vars import SEGMENT_GCS, SHARED_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import PROJECT_CRS

def create_shapes_to_roads_crosswalk(
    analysis_date: str, 
    road_segments_path: str,
) -> pd.DataFrame:
    """
    Spatial join road segments with shapes
    to pare down the road segments we're interested in.
    """
    keep_road_cols = [*GTFS_DATA_DICT.road_segments.segment_identifier_cols]
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        get_pandas = True,
        crs = PROJECT_CRS
    ).pipe(
        helpers.remove_shapes_outside_ca
    )
    
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(25)
    )
                
    road_segments = gpd.read_parquet(
        f"{SHARED_GCS}{road_segments_path}.parquet",
        columns = keep_road_cols + ["geometry"]
    ).to_crs(PROJECT_CRS)
            
    shapes_to_roads = gpd.sjoin(
        shapes,
        road_segments,
        how = "inner",
        predicate = "intersects"
    )[
        ["shape_array_key"] + keep_road_cols
    ].drop_duplicates().reset_index(drop=True)
    
    shapes_to_roads.to_parquet(
        f"{SEGMENT_GCS}roads_staging/"
        f"shape_road_crosswalk_{analysis_date}.parquet"
    )
    
    return

    
if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date
    
    LOG_FILE = "../logs/sjoin_shapes_roads.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    ROAD_SEGMENTS = GTFS_DATA_DICT.shared_data.road_segments_onekm
    
    for analysis_date in [analysis_date]:
        
        start = datetime.datetime.now()
        
        create_shapes_to_roads_crosswalk(
            analysis_date, 
            ROAD_SEGMENTS
        )
        
        end = datetime.datetime.now()
        logger.info(
            f"create shape to road crosswalk: {analysis_date}: {end - start}")

