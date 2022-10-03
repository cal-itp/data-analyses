# Geometry
from shared_utils import geography_utils
from shared_utils import utils
import geopandas as gpd
import fsspec
from calitp import *

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/cellular_coverage/"

# Clip the data coverage map to California only.
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
  
    # Snake case & drop columns
    unwanted = ["dba","technology","mindown","minup"]
    fcc_ca_gdf = to_snakecase(fcc_ca_gdf)
    fcc_ca_gdf = fcc_ca_gdf.drop(columns = unwanted)
    
    # Save this into a parquet so don't have to clip all the time
    utils.geoparquet_gcs_export(fcc_ca_gdf, GCS_FILE_PATH, new_file_name)

# Return a cleaned up NTD dataframe 
def ntd_vehicles():
    
    # Open sheet
    df = pd.read_excel(
    f"gs://calitp-analytics-data/data-analyses/5311 /2020-Vehicles_1.xlsm",
    sheet_name="Vehicle Type Count by Agency",)
    
    # Only grab California
    df = df.loc[ntd_df2["state"] == "CA"]
    
    # Only get bus related columns
    columns_wanted = [
    "Agency",
    "State",
    "Bus",
    "Over-The-Road Bus",
    "Articulated Bus",
    "Double Decker Bus",
    "School Bus",
    "Van",
    "Cutaway",
    "Minivan"]
    
    # Have to add snakecase after b/c some columns have integers
    # Drop unwanted columns
    df = to_snakecase(df[columns_wanted])
    
    # Add up buses
    df["total_buses"] = df.sum(numeric_only=True, axis=1)
    
    # Drop agencies with 0 buses
    df = df.loc[ntd_df2['total_buses'] !=0]
    
    return df

# Find unique routes 
def unique_routes(gdf) -> gpd.GeoDataFrame:
    gdf = gdf.assign(
        route_length=(gdf.geometry.to_crs(geography_utils.CA_StatePlane).length)
    )

    unique_route = (
        gdf.sort_values(
            ["itp_id", "route_id", "route_length"], ascending=[True, True, False]
        )
        .drop_duplicates(subset=["itp_id", "route_id"])
        .reset_index(drop=True)[
            ["itp_id", "route_id", "geometry", "route_type",
             "route_name", "agency", "route_length"]
        ]
    )
    
    # Filter out for bus only 
    unique_route = unique_route.loc[unique_route["route_type"] == "3"]
    
    # Filter out for any Amtrak records
    unique_route = unique_route.loc[unique_route["agency"] != "Amtrak"]
    
    return unique_route

# Overlap
def comparison(gdf_left, gdf_right):

    # Overlay
    overlay_df = gpd.overlay(
        gdf_left, gdf_right, how="intersection", keep_geom_type=False
    )

    # Create route length again? Why?
    overlay_df = overlay_df.assign(
        route_length=overlay_df.geometry.to_crs(geography_utils.CA_StatePlane).length
    )

    return overlay_df