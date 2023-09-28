"""
Import, concatenate individual parquets.
Add a couple of columns (identifier, region) and 
also downgrade dtypes for columns to save memory.
"""
import os
os.environ['USE_PYGEOS'] = '0'

import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger
from calitp_data_analysis import utils

GCS_FILE_PATH = "gs://calitp-publish-data-analysis/py_crow_flies/"
CRS = "EPSG:3857"

files = ["CentralCal_POIs", "Mojave_POIs", 
         "NorCal_POIs", "SoCal_POIs"]

def import_file(filename: str) -> gpd.GeoDataFrame:
    """
    Import region's POI parquet.
    Add new column that shows what region it's from.
    """
    gdf = gpd.read_parquet(
        f"{GCS_FILE_PATH}{filename}.parquet"
    )
    
    gdf = gdf.assign(
        region = f"{filename.split('_')[0]}",
        x = gdf.geometry.x, 
        y = gdf.geometry.y
    )

    return gdf


def prep_and_concatenate_pois_by_region(files: list) -> gpd.GeoDataFrame:
    """
    Import file and concatenate into 1 gdf and save to GCS.
    """
    # Import and add column for region
    dfs = [import_file(f) for f in files]

    # Concatenate 
    gdf = pd.concat(dfs).reset_index(drop=True)

    # Assign unique identifier that's integer
    gdf = gdf.assign(
        poi_index = gdf.index
    )
    
    # Adjust data types and downgrade to save memory
    # https://towardsdatascience.com/reducing-memory-usage-in-pandas-with-smaller-datatypes-b527635830af
    gdf = gdf.astype({
        "pointid": "int32",
        "poi_index": "int32",
        "grid_code": "int16"
    })
        
    return gdf


if __name__ == "__main__":    
    
    LOG_FILE = "./logs/t1_data_prep.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()

    gdf = prep_and_concatenate_pois_by_region(files)
    
    time1 = datetime.datetime.now()
    logger.info(f"prep and concatenate regions: {time1-start}")
    
    # Save in GCS
    utils.geoparquet_gcs_export(
        gdf,
        GCS_FILE_PATH,
        "all_pois"
    )
    end = datetime.datetime.now()
    logger.info(f"export full gdf to GCS: {end-time1}")
    logger.info(f"execution time: {end - start}")
    