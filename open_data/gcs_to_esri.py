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


def horiz_accuracy_decimal_places(gdf, decimal_degrees_string):
    '''
    0.00004 decimal degrees (4 meters) for `stops` and anything derived from that
    0.001 decimal degrees (100 meters)
    
    If we had point data, we could probably cast it to floats, 
    round, then convert back to geometry
    
    But, no real way of rounding geometry column to appropriate decimal places
    
    For the most part, we are at 5 decimal places for stops, so that's fine
    TODO: Think more about shapes though...do we want 3 decimal places? 
    100 m is how far the shapes path must be from the stop. But it doesn't mean how far it
    is from the actual alignment / the path being represented.
    
    https://stackoverflow.com/questions/6189956/easy-way-of-finding-decimal-places
    '''
    fraction = float(decimal_degrees_string.replace(' decimal degrees', ''))
    decimal_places = abs(int(f'{fraction:e}'.split('e')[-1]))
    
    gdf = gdf.to_crs(geography_utils.WGS84)
    gdf = gdf.assign(
        x = round(gdf.geometry.x, decimal_places),
        y = round(gdf.geometry.y, decimal_places),
    ).drop(columns = "geometry")
    
    gdf = geography_utils.create_point_geometry(
        gdf, 
        longitude_col = "x", latitude_col = "y",
        crs = geography_utils.WGS84
    ).drop(columns = ["x", "y"])
    
    
    return gdf

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

