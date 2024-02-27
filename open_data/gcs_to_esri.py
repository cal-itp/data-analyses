"""
Take datasets staged in GCS, create zipped shapefiles.

Print some info about the gdf
Double check metadata, esp field attributes and EPSG
"""
import geopandas as gpd
import glob
import intake
import os
import sys

from loguru import logger

import publish_utils
from calitp_data_analysis import utils, geography_utils
from update_vars import analysis_date, RUN_ME

catalog = intake.open_catalog("./catalog.yml")

def print_info(gdf: gpd.GeoDataFrame):
    """
    Double check that the metadata is entered correctly and 
    that dtypes, CRS, etc are all standardized.
    """
    logger.info(f"CRS Info: {gdf.crs.name}, EPSG: {gdf.crs.to_epsg()}")
    logger.info(f"columns: {gdf.columns}")
    logger.info(f"{gdf.dtypes}")
    
    return
    
    
def remove_zipped_shapefiles():
    """
    Once local zipped shapefiles are created, 
    clean up after we don't need them
    """
    FILES = [f for f in glob.glob("*.zip")]
    print(f"list of parquet files to delete: {FILES}")
    
    for f in FILES:
        os.remove(f)
    return
    
    
if __name__=="__main__":
    assert os.getcwd().endswith("open_data"), "this script must be run from open_data directory!"

    logger.add("./logs/gcs_to_esri.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
        
    for d in RUN_ME :
        gdf = catalog[d].read().to_crs(geography_utils.WGS84)
        gdf = publish_utils.standardize_column_names(gdf).pipe(
            publish_utils.remove_internal_keys)

        logger.info(f"********* {d} *************")
        print_info(gdf)

        # Zip the shapefile
        utils.make_zipped_shapefile(gdf, f"{d}.zip")
    


