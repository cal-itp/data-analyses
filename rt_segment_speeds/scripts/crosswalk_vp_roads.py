"""
Create crosswalk of local roads to vp path
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger
from calitp_data_analysis.sql import to_snakecase

from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS, SHARED_GCS

def create_vp_roads_crosswalk(
    analysis_date: str,
    buffer_meters: int,
    **kwargs
) -> dd.DataFrame:
    """
    Get a crosswalk of local roads to vp path.
    """
    VP_PROJECTED_FILE = GTFS_DATA_DICT.modeled_vp.vp_projected
    
    vp_path = dg.read_parquet(
        f"{SEGMENT_GCS}{VP_PROJECTED_FILE}_{analysis_date}.parquet",
        columns = ["trip_instance_key", "vp_geometry"],
    ).set_geometry("vp_geometry")
    
    roads = dg.read_parquet(
        f"{SHARED_GCS}all_roads_2020_state06.parquet",
        columns=["LINEARID", "MTFCC", "FULLNAME", "geometry"],
        **kwargs
    ).pipe(to_snakecase).to_crs(PROJECT_CRS)
    
    roads = roads.repartition(npartitions=25)

    roads = roads.assign(
        geometry = roads.geometry.buffer(buffer_meters)
    )
    
    vp_path_on_roads = dg.sjoin(
        vp_path,
        roads,
        how = "inner",
        predicate = "intersects"
    )[["trip_instance_key", "linearid", "mtfcc", "fullname"]].drop_duplicates()
    
    return vp_path_on_roads
    
    
if __name__ == "__main__":
    LOG_FILE = "../logs/modeled_road_speeds.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")   
    
    from segment_speed_utils.project_vars import test_dates
    
    for analysis_date in test_dates:
        start = datetime.datetime.now()

        EXPORT_FILE = GTFS_DATA_DICT.modeled_road_segments.crosswalk
        BUFFER_METERS = 35
        
        crosswalk = create_vp_roads_crosswalk(
            analysis_date,
            BUFFER_METERS,
        ).persist()
        
        # Splitting local roads from major ones doesn't seem to impact times
        crosswalk = crosswalk.compute()
    
        crosswalk.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
        )
    
        end = datetime.datetime.now()
        logger.info(f"crosswalk vp to roads: {end - start}")
    
    