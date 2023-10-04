from calitp_data_analaysis import utils
import geopandas as gpd
import dask.dataframe as dd
import dask_geopandas as dg
import pandas as pd
import shapely.wkt
from calitp_data_analysis.sql import to_snakecase

# Open zip files 
import fsspec
from calitp_data_analysis import get_fs 
fs = get_fs()
import os

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/cellular_coverage/"

# Times
import datetime
from loguru import logger

"""
Clip original provider maps
to California
"""
# Clip the cell provider coverage map to California only.
# This only worked for AT&T and Verizon. 
# T-Mobile was concatted as it was spread across several files instead of one. 
def create_california_coverage(file_zip_name:str, new_file_name:str):
    
    PATH = f"{GCS_FILE_PATH}{file_zip_name}"
    with fsspec.open(PATH) as file:
        fcc_gdf = gpd.read_file(file)
    
    # Open file with California Counties.
    ca_gdf = gpd.read_file(
    "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson")
    
    # Clip to only California 
    # https://fcc.maps.arcgis.com/apps/webappviewer/index.html?id=6c1b2e73d9d749cdb7bc88a0d1bdd25b
    fcc_ca_gdf = gpd.clip(fcc_gdf, ca_gdf)
  
    # Snake case & drop columns
    unwanted = ["dba","technology","mindown","minup"]
    fcc_ca_gdf = to_snakecase(fcc_ca_gdf)
    fcc_ca_gdf = fcc_ca_gdf.drop(columns = unwanted)
    
    # Save this into a parquet so don't have to clip all the time
    utils.geoparquet_gcs_export(fcc_ca_gdf, GCS_FILE_PATH, new_file_name)
    
"""
Take clipped California 
maps and manipulate them. 
"""
def find_specific_files(phrase_to_find:str):
    """
    Grab a list of files that contain the 
    phrase inputted. E.g. "tmobile_no_coverage"
    """
    # Create a list of all the files in my folder
    all_files_in_folder = fs.ls(A1_provider_prep.GCS_FILE_PATH)
    
    # Grab only files with the string "Verizon_no_coverage_"
    my_files = [i for i in all_files_in_folder if phrase_to_find in i]
    
    # String to add to read the files
    my_string = "gs://"
    my_files = [my_string + i for i in my_files]
   
    return my_files

# Sjoin the original provider gdf to a single district.
def sjoin_district(
    provider_df: dg.GeoDataFrame, 
    district_df: gpd.GeoDataFrame,
    provider_name: str,
) -> dg.GeoDataFrame:
    
    # Clip provider to CT district
    provider_district = dg.sjoin(
        provider_df, 
        district_df, 
        how="inner", 
        predicate="intersects"
    ).drop(columns = "index_right")
    
    # Compute back to normal gdf
    provider_district = provider_district.compute()
    
    # Stash intermediate output here 
    d = provider_district.district.iloc[0]
    utils.geoparquet_gcs_export(provider_district, GCS_FILE_PATH, f"{provider_name}_d{d}")
    print(f"saved {provider_name}_d{d} parquet") 
    
    return provider_district

# Sjoin all districts to the provider map.
def sjoin_gdf(
    provider_df: dg.GeoDataFrame, 
    district_df: gpd.GeoDataFrame,
    file_name: str,
    districts_needed:list) -> dg.GeoDataFrame:
    
    full_gdf = pd.DataFrame()
    
    for i in districts_needed:
        result = sjoin_district(
            provider_df, 
            district_df[district_df.district==i],
            provider_name
        )

        full_gdf = dd.multi.concat([full_gdf, result], axis=0)
    
    full_gdf = full_gdf.compute()
    
    utils.geoparquet_gcs_export(full_gdf, GCS_FILE_PATH, file_name)
    return full_gdf

def clip_sjoin_gdf(files_to_find:str, provider: str):
    """
    Provider maps were spatially joined against each Caltrans
    District using the function `sjoin_district`. 
    However, these sjoin gdfs include areas that don't belong 
    to that district, which throw off results.
    Clip these files by provider against the the original CT
    shapefile to clean up the edges. 
    
    files_to_find: find files that were spatially joined
    to the CT districts. 
    
    provider: the provider
    """
    # Open original Caltrans districts shapefile
    # Get rid of A1_provider_prep once I export this
    ct_districts = get_districts()
    
    # Get a list of files I want
    provider_files_list = find_specific_files(files_to_find)
    
    # Loop over every file
    # Put provider_files_list later.
    for file in provider_files_list:
        # Find which district each file contains. 
        # https://stackoverflow.com/questions/11339210/how-to-get-integer-values-from-a-string-in-python
        relevant_district = ''.join(i for i in file if i.isdigit())
        
        # Turn this into an integer
        relevant_district = int(relevant_district)
        
        # Filter out districts for the file's district
        relevant_district_gdf =  ct_districts[ct_districts.district == relevant_district]
        
        # Open file
        sjoin_file = gpd.read_parquet(file)
        
        # Clip the sjoin file against the original district shapefile
        clipped_gdf = sjoin_file.clip(relevant_district_gdf)
        
        # Save
        utils.geoparquet_gcs_export(clipped_gdf, GCS_FILE_PATH, f"{provider}_clip_d{relevant_district}")
        
        print(f"Done for {relevant_district}") 

def dissolve_clipped_gdf(phrase_to_find:str, provider: str):
    """
    Input files created by `clip_sjoin_gdf` 
    and dissolve all the rows by district.
    """
    # Get a list of files I want
    provider_files_list = find_specific_files(phrase_to_find)
    
    # Loop over every file
    # Put provider_files_list later.
    for file in provider_files_list:
        
        start = datetime.datetime.now()
        
        # Open file
        clipped_dask = dg.read_parquet(file)
        
        # Grab the district
        relevant_district = ''.join(i for i in file if i.isdigit())
        
        # Turn this into an integer for the file name
        relevant_district = int(relevant_district)
        
        # Dissolve by district
        dissolved_dask = clipped_dask.dissolve("district")
        
        # Turn back to gdf
        dissolved_gdf = dissolved_dask.compute()
        
        # Save
        utils.geoparquet_gcs_export(dissolved_gdf, GCS_FILE_PATH, f"{provider}_dissolve_d{relevant_district}")
        
        end = datetime.datetime.now()
        
        logger.info(f"execution time: {end-start}")
        print(f"Done for {relevant_district}") 
        
def find_difference_gdf(phrase_to_find:str, provider: str):
    """
    Input the files created by `dissolve_clipped_gdf` 
    and find the difference with the original Caltrans
    district. 
    """
    # Open original Caltrans districts shapefile
    # Get rid of A1_provider_prep once I export this
    ct_districts = A1_provider_prep.get_districts()
    
    # Get a list of files I want
    provider_files_list = find_specific_files(phrase_to_find)
    
    start = datetime.datetime.now()
    
    # Loop over every file
    for file in provider_files_list:
        
        relevant_district = ''.join(i for i in file if i.isdigit())
        
        relevant_district = int(relevant_district)
        
        # Filter out districts for the file's district
        relevant_district_gdf =  ct_districts[ct_districts.district == relevant_district]
        
        # Open file
        dissolved_file = gpd.read_parquet(file)
        
        # Clip the dissolved file against the original district shapefile
        no_coverage = relevant_district_gdf.difference(dissolved_file.geometry.iloc[0]).reset_index()
        
        try:
            # Some are geodataframes without column names: have to rename them. 
            no_coverage = no_coverage.rename(columns = {0: 'geometry'})
            utils.geoparquet_gcs_export(no_coverage, GCS_FILE_PATH, f"{provider}_difference_d{relevant_district}")
        except:
            # Some become geoseries - turn them into a gdf
            # https://gis.stackexchange.com/questions/266098/how-to-convert-a-geoseries-to-a-geodataframe-with-geopandas
            no_coverage_gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries(no_coverage))
            utils.geoparquet_gcs_export(no_coverage_gdf, GCS_FILE_PATH, f"{provider}_difference_d{relevant_district}")
        
        print(f"Done for {relevant_district}") 
            
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
        
    return no_coverage

def stack_all_maps(phrase_to_find:str, provider: str):
    """
    After running `find_difference_gdf`, the results are 
    seperated out by districts. Stack all the district files to create 
    one complete California file for this provider.
    """
    # Get a list of files I want
    provider_files_list = find_specific_files(phrase_to_find)
    
    start = datetime.datetime.now()
    
    # Concat files
    california_map = dd.multi.concat([gpd.read_parquet(file) for file in provider_files_list])
    california_map = california_map.compute()
    
    utils.geoparquet_gcs_export(california_map, GCS_FILE_PATH, f"{provider}_no_coverage_cal")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
        
    return california_map

"""
Final Provider Files
"""
def load_att(): 
    att_file =  "att_no_coverage_cal.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{att_file}")
    return gdf

# Areas that don't have Verizon cell coverage across CA
def load_verizon(): 
    verizon_file =  "verizon_no_coverage_cal.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{verizon_file}")
    return gdf

# Areas that don't have T-mobile cell coverage across CA
def load_tmobile(): 
    tmobile_file =  "tmobile_no_coverage_cal.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{tmobile_file}")
    return gdf

"""
CA Counties & Districts Files
"""
# CT shapefile
caltrans_shape = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"

# Open a file with shapes of CA districts
def get_districts():
    df = to_snakecase(
        gpd.read_file(f"{caltrans_shape}").to_crs(epsg=4326)
    )[["district", "geometry"]]
    return df

# Open a file with shapes of CA counties
def get_counties():
    ca_gdf = (
        "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson"
    )
    
    my_gdf = to_snakecase(gpd.read_file(f"{ca_gdf}")
                          .to_crs("EPSG:4326"))[
        ["county_name", "geometry"]
    ]
    
    return my_gdf

# Kern County plots incorrectly - correct it 
def correct_kern():
    counties = get_counties()
    
    # Grab only Kern County
    kern = counties.loc[counties.county_name == "Kern"].reset_index(drop=True)
    
    # Non node intersection line string error - fix Kern County
    # https://github.com/geopandas/geopandas/issues/1724
    kern["geometry"] = kern["geometry"].apply(
    lambda x: shapely.wkt.loads(shapely.wkt.dumps(x, rounding_precision=4)))
    
    return kern 
