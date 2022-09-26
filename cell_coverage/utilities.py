# Geometry
from shared_utils import geography_utils
import geopandas as gpd
import fsspec
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/cellular_coverage/"

# Function to clip the data coverage map to California only.
def create_california_coverage(file_zip_name:str, new_file_name:str):
    
    # Open zip file first
    PATH = f"{GCS_FILE_PATH}{file_zip_name}"
    with fsspec.open(PATH) as file:
        fcc_gdf = gpd.read_file(file)
    
    # Open file with California Counties.
    ca_gdf = gpd.read_file(
    "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson")

    # Clip 
    fcc_ca_gdf = gpd.clip(fcc_gdf, ca_gdf)

    # Save this into a parquet so don't have to clip all the time
    utils.geoparquet_gcs_export(fcc_ca_gdf, GCS_FILE_PATH, new_file_name)
