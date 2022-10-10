from shared_utils import geography_utils
from shared_utils import utils
import geopandas as gpd
import pandas as pd
import fsspec
from calitp import *

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/cellular_coverage/"

"""
FCC data
"""
# Clip the data coverage map for California only.
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

"""
Routes DF
"""
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
    
    # Fill in NA for route names
    unique_route["route_name"] = unique_route["route_name"].replace({"": "None"})
    
    return unique_route


"""
Open/Clean Files
"""
# Open AT&T coverage shapefile that's already clipped to California
def load_att(): 
    att_file =  "att_ca_only.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{att_file}")
    return gdf

# Open Verizon coverage shapefile that's already clipped to California
def load_verizon(): 
    verizon_file =  "verizon_ca_only.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{verizon_file}")
    return gdf

# Open AT&T coverage shapefile that's already clipped to California
def load_tmobile(): 
    tmobile_file =  "tmobile_california.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{tmobile_file}")
    return gdf

# Open routes file, find unique routes
def load_unique_routes_df():
    routes_file =  "gs://calitp-analytics-data/data-analyses/traffic_ops/ca_transit_routes.parquet"
    df = gpd.read_parquet(routes_file)
    
    # Find unique routes
    df =  unique_routes(df)
    
    # Standardize route id  
    df["route_id"] = df["route_id"].str.lower().str.strip()
    
    return df

def load_clean_trips_df():
    
    # Read in file
    trips_file = "gs://calitp-analytics-data/data-analyses/rt_delay/compiled_cached_views/trips_2022-05-04_all.parquet"
    df = pd.read_parquet(trips_file)
    
    # Standardize route id  
    df["route_id"] = df["route_id"].str.lower().str.strip()
    
    # Aggregate trips_df: count each trip id based on unique? 
    df2 = (df
             .groupby(['calitp_itp_id', 'route_id'])
             .agg({'trip_id':'nunique'})
             .reset_index()
             .rename(columns = {'trip_id':'total_trips'})
            )
    
    return df2
    
"""
Analysis Functions
"""
# Overlay FCC map with original bus routes df.
def comparison(gdf_left, gdf_right):

    # Overlay
    overlay_df = gpd.overlay(
        gdf_left, gdf_right, how="intersection", keep_geom_type=False
    )

    # Create a new route length
    overlay_df = overlay_df.assign(
        route_length=overlay_df.geometry.to_crs(geography_utils.CA_StatePlane).length
    )

    return overlay_df

# Take the FCC provider shape file, compare it against the original df
# Find % of route covered by a provider compared to the original route length.

def route_cell_coverage(provider_gdf, original_routes_df, suffix: str):
    """
    Args:
        provider_gdf: the provider gdf clipped to CA
        original_routes_df: the original df with all the routes
        suffix (str): suffix to add behind dataframe
    Returns:
        Returns a gdf with the percentage of the routes covered by a provider
    """
    # Overlay the dfs
    overlay = comparison(original_routes_df, provider_gdf)

    # Sum up lengths of routes by route id, name, agency, and itp_id
    overlay2 = (
        overlay.groupby(["route_id", "route_name", "agency", "itp_id"])
        .agg({"route_length": "sum"})
        .reset_index()
    )

    # Merge original dataframe with old route with provider-route overlay
    # To compare original route length and old route length
    m1 = pd.merge(
        overlay2,
        original_routes_df,
        how="inner",
        on=["agency", "route_id", "route_name", "itp_id"],
        suffixes=["_overlay", "_original_df"],
    )
    
    # Create % of route covered vs. not 
    m1["percentage"] = (
        m1["route_length_overlay"] / m1["route_length_original_df"]
    ) * 100
    
    # Create bins for analysis
    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    m1["binned"] = pd.cut(m1["percentage"], bins)
    
    m1 =  m1.add_suffix(suffix)
    return m1

"""
NTD
"""
# Clean organization names - strip them of dba, etc
def organization_cleaning(df, column_wanted: str):
    df[column_wanted] = (
        df[column_wanted]
        .str.strip()
        .str.split(",")
        .str[0]
        .str.replace("/", "")
        .str.split("(")
        .str[0]
        .str.split("/")
        .str[0]
    )
    return df

# Return a cleaned up NTD dataframe for bus only 
def ntd_vehicles():
    
    # Open sheet
    df = pd.read_excel(
    f"gs://calitp-analytics-data/data-analyses/5311 /2020-Vehicles_1.xlsm",
    sheet_name="Vehicle Type Count by Agency",)
    
    # Only grab California
    df = df.loc[df["State"] == "CA"]
    
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
    
    # Clean org names
    df = organization_cleaning(df, 'agency') 
    
    # Add up buses
    df["total_buses"] = df.sum(numeric_only=True, axis=1)
    
    # Drop agencies with 0 buses
    df = df.loc[df['total_buses'] !=0]
    
    return df
