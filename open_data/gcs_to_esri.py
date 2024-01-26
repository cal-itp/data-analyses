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
from shared_utils import portfolio_utils
from update_vars import analysis_date

catalog = intake.open_catalog("./catalog.yml")

def standardize_column_names(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize how agency is referred to.
    """
    RENAME_DICT = {
        "caltrans_district": "district_name"
    }
    # these rename hqta datasets
    # agency_name_primary, agency_name_secondary, etc
    df.columns = df.columns.str.replace('agency_name', 'agency')
    
    df = df.rename(columns = RENAME_DICT)
    df
    
    return df


def remove_internal_keys(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Remove columns used in our internal data modeling.
    Leave only natural identifiers (route_id, shape_id).
    Remove shape_array_key, gtfs_dataset_key, etc.
    """
    exclude_list = ["sec_elapsed", "meters_elapsed"]
    cols = [c for c in df.columns]
    
    internal_cols = [c for c in cols if "_key" in c or c in exclude_list] 
    
    print(f"drop: {internal_cols}")
    
    return df.drop(columns = internal_cols)
    

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
    
    #datasets = list(dict(catalog).keys())
    datasets = open_data.RUN_ME 
    
    for d in datasets :
        gdf = catalog[d].read().to_crs(geography_utils.WGS84)
        gdf = standardize_column_names(gdf).pipe(remove_internal_keys)

        logger.info(f"********* {d} *************")
        print_info(gdf)

        # Zip the shapefile
        utils.make_zipped_shapefile(gdf, f"{d}.zip")
    


