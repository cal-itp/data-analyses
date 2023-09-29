"""
Ingest roads file for CA.

Options: 
- Open Street Map - all CA Roads (lines and polygons)
Read in zipped shapefile, save as geoparquet immediately.
https://data.humdata.org/dataset/hotosm_usa_california_roads

Don't use OSM. This is block-level lines, so it's nearly impossible
to wrangle to our road segments, combining it would create
a multi-linestring and too many points in between.

- Census TIGER roads - by county (lines)
Read in zipped shapefile, save out county geoparquets, 
then concat immediately.
https://www2.census.gov/geo/tiger/TIGER2020/ROADS/
"""
import dask.dataframe as dd
import dask_geopandas as dg
import fsspec
import geopandas as gpd

from calitp_data_analysis import utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/shared_data/"

def download_ca_osm():
    file_name = "hotosm_usa_california_roads_lines_shp"
    PATH = f"{GCS_FILE_PATH}{file_name}.zip"        

    with fsspec.open(PATH) as f:       
        gdf = gpd.read_file(f)
        
    # Save partitioned dask gdf locally, in case it doesn't work above
    #gddf = dg.from_geopandas(gdf, npartitions=20)
    #gddf.to_parquet(f"{file_name}/")

    # Export as geoparquet
    utils.geoparquet_gcs_export(
        gdf,
        GCS_FILE_PATH,
        file_name
    )
    

def get_ca_county_fips() -> list:
    ca_county_fips = []
    for i in range(1, 117, 2):
        fips_str = str(i).zfill(3)
        ca_county_fips.append(fips_str)
    
    return ca_county_fips
    
def download_census_tiger_roads(
    year: int = 2020, 
    state_fips: str = "06", 
    county_fips_list: list = [],
):    
    """
    Loop through the counties for a given state, 
    read in the URL directly for TIGER Roads zipped shapefile.
    
    Export immediately as geoparquet in GCS.
    """
    for county_fips in county_fips_list:
        file_name = f"tl_{year}_{state_fips}{county_fips}_roads"
        
        URL = (
            "https://www2.census.gov/geo/tiger/TIGER2020/ROADS/"
            f"{file_name}.zip"
        )
        
        with fsspec.open(URL) as f:       
            gdf = gpd.read_file(f)
            
        # Export as geoparquet
        utils.geoparquet_gcs_export(
            gdf,
            f"{GCS_FILE_PATH}ca_roads/",
            file_name
        )
        
        print(f"Exported {county_fips}")

        
def concatenate_all_counties(
    year: int = 2020, 
    state_fips: str = "06", 
    county_fips_list: list = [],
):
    """
    Concatenate all the geoparquets into 1 parquet.
    """

    # Set metadata for dask gdf with reading first gdf in
    first_county = county_fips_list[0]
    all_roads = dg.read_parquet(
        f"{GCS_FILE_PATH}ca_roads/"
        f"tl_{year}_{state_fips}{first_county}_roads.parquet")
    
    for county_fips in county_fips_list[1:]:
        file_name = f"tl_{year}_{state_fips}{county_fips}_roads"

        county_gdf = dg.read_parquet(
            f"{GCS_FILE_PATH}ca_roads/{file_name}.parquet")
        
        all_roads = dd.multi.concat([all_roads, county_gdf], axis=0)
    
    all_roads = all_roads.reset_index(drop=True).compute()
    
    utils.geoparquet_gcs_export(
        all_roads,
        GCS_FILE_PATH,
        f"all_roads_{year}_state{state_fips}"
    )

    
if __name__ == "__main__":
    
    # Set up list of FIPs codes to loop through and read in as gdf, 
    # then save to GCS as geoparquet
    YEAR = 2020
    CA_FIPS = "06"
    ca_county_fips = get_ca_county_fips()
    
    print(ca_county_fips)

    download_census_tiger_roads(
        year = YEAR, 
        state_fips = CA_FIPS, 
        county_fips_list = ca_county_fips
    )
    
    concatenate_all_counties(
        year = YEAR, 
        state_fips = CA_FIPS, 
        county_fips_list = ca_county_fips
    )


        
        