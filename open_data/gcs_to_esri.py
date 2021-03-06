"""
Take datasets staged in GCS, create zipped shapefiles.

Print some info about the gdf
Double check metadata, esp field attributes and EPSG
"""
import geopandas as gpd
import glob
import intake
import os

from shared_utils import utils, geography_utils

catalog = intake.open_catalog("./*.yml")


def standardize_column_names(df):
    df.columns = df.columns.str.replace('calitp_itp_id', 'itp_id')
    df.columns = df.columns.str.replace('agency_name', 'agency')
    return df


# Use this to double-check metadata is entered correctly
def print_info(gdf):
    print(f"CRS Info: {gdf.crs.name}, EPSG: {gdf.crs.to_epsg()}")
    print(f"columns: {gdf.columns}")
    print(f"{gdf.dtypes}")
    
    
# Once local zipped shapefiles are created, clean up after we don't need them    
def remove_zipped_shapefiles():
    FILES = [f for f in glob.glob("*.zip")]
    print(f"list of parquet files to delete: {FILES}")
    
    for f in FILES:
        os.remove(f)    
    
    
if __name__=="__main__":
    assert os.getcwd().endswith("open_data"), "this script must be run from open_data directory!"

    datasets = list(dict(catalog).keys())
    
    for d in datasets:
        gdf = catalog[d].read().to_crs(geography_utils.WGS84)
        gdf = standardize_column_names(gdf)

        print(f"********* {d} *************")
        print_info(gdf)

        # Zip the shapefile
        utils.make_zipped_shapefile(gdf, f"{d}.zip")
    
    # Clean up local files
    #remove_zipped_shapefiles()

