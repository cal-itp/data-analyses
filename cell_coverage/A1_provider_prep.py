from shared_utils import geography_utils
from shared_utils import utils
import geopandas as gpd
import dask.dataframe as dd
import dask_geopandas as dg
import pandas as pd
import shapely.wkt
# Open zip files 
import fsspec
from calitp import *
from calitp.storage import get_fs
fs = get_fs()
import os

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/cellular_coverage/"

"""
Federal Communications Commission
Data Wrangling
"""
# Clip the cell provider coverage map to California only.
# This only worked for AT&T and Verizon. T-Mobile uses a different function.
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

# Clip the provider map to a boundary and return the areas of a boundary
# that is NOT covered by the provider.
def find_difference_and_clip(
    gdf: dg.GeoDataFrame, boundary: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    # Clip cell provider to some boundary
    clipped = dg.clip(gdf, boundary).reset_index(drop=True)  # use dask to clip
    clipped_gdf = clipped.compute()  # compute converts from dask gdf to gdf

    # Now find the overlay, and find the difference
    # Notice which df is on the left and which is on the right
    # https://geopandas.org/en/stable/docs/user_guide/set_operations.html
    no_coverage = gpd.overlay(boundary, clipped_gdf, how="difference",  keep_geom_type=False)

    return no_coverage

def concat_all_areas(all_gdf:list, gcs_file_path: str, file_name:str):
    """
    Districts/counties are separated out into different gdfs that contain 
    portions of districts/counties. Concat them all together 
    to get the entirety of California.
    """
    # Empty dataframe
    full_gdf = pd.DataFrame()
    
    # Concat all the districts that were broken out into one
    full_gdf = dd.multi.concat(all_gdf, axis=0)
    
    # Turn it into a gdf
    full_gdf = full_gdf.compute()
    
    # Export
    geoparquet_gcs_export(full_gdf, gcs_file_path,file_name)

    print('Saved to GCS')
    return full_gdf 

# Breakout provider gdf by counties, find the areas of each county
# that doesn't have coverage, concat everything and dissolve to one row.
# This was used for Verizon ONLY to create its final map.
def breakout_counties(provider, gcs_file_path:str, file_name:str, counties_wanted:list):
    counties = get_counties()
    
    # Empty dataframe to hold each district after clipping
    full_gdf = pd.DataFrame()

    for i in counties_wanted:
        county_gdf = counties[counties.county_name==i].reset_index(drop = True)
        
        county_gdf_clipped = find_difference_and_clip(verizon, county_gdf) 
        full_gdf = dd.multi.concat([full_gdf, county_gdf_clipped], axis=0)
        print(f'done concating for {i}')
    
    # Turn this into a GDF
    full_gdf = full_gdf.compute()
    
    # Save to GCS
    geoparquet_gcs_export(full_gdf, gcs_file_path, file_name) 
    print('saved to GCS')
    
    return full_gdf

# Sjoin the provider gdf to a single district then & find the difference
def iloc_find_difference_district(
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
    
    # Get areas without coverage
    no_coverage = provider_district.difference(
        district_df.geometry.iloc[0], 
    ).reset_index()
    
    # Turn to gdf
    no_coverage = (no_coverage.reset_index()
                  .dissolve()
                  .rename(columns = {0: 'geometry'})
                  [["geometry"]]
                 )
    # Set geometry
    no_coverage = no_coverage.set_geometry('geometry')
    
    utils.geoparquet_gcs_export(no_coverage, GCS_FILE_PATH, f"{provider_name}_no_coverage_d{d}")
    
    print(f"{provider_name}_no_coverage_d{d} parquet")
    
    return no_coverage

# For the entirety of California by districts get areas without coverage.
# This was used for AT&T and T-Mobile's final maps.
def complete_difference_provider_district_level(
    provider_df: dg.GeoDataFrame, 
    district_df: gpd.GeoDataFrame,
    provider_name: str) -> dg.GeoDataFrame:
    
    full_gdf = pd.DataFrame()
    
    for i in [*range(1, 13, 1)]:
        result = iloc_find_difference_district(
            provider_df, 
            district_df[district_df.district==i],
            provider_name
        )

        full_gdf = dd.multi.concat([full_gdf, result], axis=0)
    
    full_gdf = full_gdf.compute()
    
    utils.geoparquet_gcs_export(full_gdf, GCS_FILE_PATH, f"{provider_name}_no_coverage_complete_CA")
    return full_gdf


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

"""
Final Provider Files
"""
# Areas that don't have AT&T cell coverage across CA
# att_all_counties.parquet was created using breakout_counties
# ATT_no_coverage_complete_CA.parquet was created using complete_difference_provider_district_level
def load_att(): 
    att_file =  "att_all_counties.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{att_file}")
    return gdf

# Areas that don't have Verizon cell coverage across CA
def load_verizon(): 
    gdf = gpd.read_parquet("gs://calitp-analytics-data/data-analyses/cellular_coverage/verizon_all_counties.parquet")
    return gdf

# Areas that don't have T-mobile cell coverage across CA
def load_tmobile(): 
    tmobile_file =  "tmobile_no_coverage_complete_CA.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{tmobile_file}")[['geometry']]
    return gdf

# Simplify provider maps
def simplify_geometry(provider: gpd.GeoDataFrame):
    # Turn to 2229
    provider = provider.to_crs(geography_utils.CA_StatePlane)

    # Simplify
    provider["geometry"] = provider.geometry.simplify(tolerance=15)

    provider = provider.to_crs(geography_utils.WGS84)

    # Keep only valid geometries
    provider = provider[provider.is_valid]

    return provider

# Load in simplified versions of all the providers
def simplify_geometry_all_providers():
    verizon = simplify_geometry(load_verizon())
    att = simplify_geometry(load_att())
    tmobile = simplify_geometry(load_tmobile())
    return verizon, att, tmobile
