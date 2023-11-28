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

import open_data
from calitp_data_analysis import utils, geography_utils
from update_vars import analysis_date

catalog = intake.open_catalog("./catalog.yml")


def standardize_column_names(df):
    df.columns = df.columns.str.replace('agency_name', 'agency')
    return df


# Use this to double-check metadata is entered correctly
def print_info(gdf):
    logger.info(f"CRS Info: {gdf.crs.name}, EPSG: {gdf.crs.to_epsg()}")
    logger.info(f"columns: {gdf.columns}")
    logger.info(f"{gdf.dtypes}")
    
    
# Once local zipped shapefiles are created, clean up after we don't need them    
def remove_zipped_shapefiles():
    FILES = [f for f in glob.glob("*.zip")]
    print(f"list of parquet files to delete: {FILES}")
    
    for f in FILES:
        os.remove(f)    
    
    
if __name__=="__main__":
    assert os.getcwd().endswith("open_data"), "this script must be run from open_data directory!"

    logger.add("./logs/gcs_to_esri.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    #datasets = list(dict(catalog).keys())
    datasets = open_data.RUN_ME 
    
    for d in datasets :
        gdf = catalog[d].read().to_crs(geography_utils.WGS84)
        gdf = standardize_column_names(gdf)

        logger.info(f"********* {d} *************")
        print_info(gdf)

        # Zip the shapefile
        utils.make_zipped_shapefile(gdf, f"{d}.zip")
    


