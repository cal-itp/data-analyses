"""
Concatenate road segments for primary/secondary/local    
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed
from loguru import logger

from update_vars import SHARED_GCS                                                 
if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date
    
    LOG_FILE = "../logs/cut_road_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
            
    start = datetime.datetime.now()
    
    keep_cols = [
        "linearid", "mtfcc", "fullname",
        "segment_sequence", "primary_direction",
        "geometry", "origin", "destination"
    ]
                
    primary_secondary = delayed(gpd.read_parquet)(
        f"{SHARED_GCS}segmented_roads_2020_primarysecondary.parquet",
        columns = keep_cols
    )
    local = delayed(gpd.read_parquet)(
        f"{SHARED_GCS}segmented_roads_2020_local.parquet",
        columns = keep_cols
    )
    
    road_segments = dd.from_delayed([primary_secondary, local])
    
    road_segments["road_length"] = road_segments.geometry.length
    
    road_segments = road_segments.assign(
        road_meters = (road_segments.groupby(["linearid", "mtfcc"],
                                observed=True, group_keys=False)
                       .road_length.cumsum().round(2)
                      )
    ).drop(
        columns = "road_length"
    ).reset_index(drop=True).repartition(npartitions=4)
        
    road_segments.to_parquet(
        f"{SHARED_GCS}road_segments", 
        overwrite = True,
        write_index=False
    )

    end = datetime.datetime.now() 
    logger.info(f"concatenate road segments: {end - start}")        
    
    
  