from shared_utils import geography_utils
from shared_utils import utils
import geopandas as gpd
import dask.dataframe as dd
import dask_geopandas as dg
import pandas as pd

# Open zip files 
import fsspec
from calitp import *
from calitp.storage import get_fs
fs = get_fs()
import os

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/cellular_coverage/"

"""
Other Functions
"""
# Export geospatial file to a geojson 
def geojson_gcs_export(gdf, GCS_FILE_PATH, FILE_NAME):
    """
    Save geodataframe as parquet locally,
    then move to GCS bucket and delete local file.

    gdf: geopandas.GeoDataFrame
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str. Filename.
    """
    gdf.to_file(f"./{FILE_NAME}.geojson", driver="GeoJSON")
    fs.put(f"./{FILE_NAME}.geojson", f"{GCS_FILE_PATH}{FILE_NAME}.geojson")
    os.remove(f"./{FILE_NAME}.geojson")


"""
FCC data
"""
# Clip the cell provider coverage map to California only.
def create_california_coverage(file_zip_name:str, new_file_name:str):
    
    # Open zip file first
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
Routes DF
"""
# Find unique routes 
def unique_routes(gdf) -> gpd.GeoDataFrame:
    gdf = gdf.assign(
        original_route_length=(gdf.geometry.to_crs(geography_utils.CA_StatePlane).length)
    )

    unique_route = (
        gdf.sort_values(
            ["itp_id", "route_id", "original_route_length"], ascending=[True, True, False]
        )
        .drop_duplicates(subset=["route_name", "route_id", "itp_id"])  
        .reset_index(drop=True)[
            ["itp_id", "route_id", "geometry", "route_type",
             "route_name", "agency", "original_route_length"]
        ]
    )
    
    # Filter out any Amtrak records
    unique_route = unique_route.loc[unique_route["agency"] != "Amtrak"]
    
    # Filter out for bus only 
    unique_route = unique_route.loc[unique_route["route_type"] == "3"]
    
    # Drop route type
    unique_route = unique_route.drop(columns = ["route_type"]) 
    
    # Fill in NA for route names
    unique_route["route_name"] = unique_route["route_name"].replace({"": "None"})
    
    return unique_route


"""
Open/Clean Files
"""
# Open a file with shapes of CA counties
def get_counties():
    # California counties.
    ca_gdf = (
        "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson"
    )
    
    my_gdf = to_snakecase(gpd.read_file(f"{ca_gdf}")
                          .to_crs("EPSG:4326"))[
        ["county_name", "geometry"]
    ]
    
    return my_gdf

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

# Open T-Mobile shapefile - NOT clipped to California b/c it took too long. 
# Includes parts of other states on the West Coast
def load_tmobile(): 
    tmobile_file =  "tmobile_california.parquet"
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}{tmobile_file}")[['geometry']]
    return gdf

# Open routes file, find unique routes
def load_unique_routes_df():
    # traffic_ops/export/ca_transit_routes_[date].parquet
    routes_file =  "gs://calitp-analytics-data/data-analyses/traffic_ops/export/ca_transit_routes_2022-09-14.parquet"
    df = gpd.read_parquet(routes_file)
    
    # Find unique routes
    df =  unique_routes(df)
    
    # Standardize route id  
    df["route_id"] = df["route_id"].str.lower().str.strip()
    
    return df

"""
# of Trips
""" 
# Find number of trips ran per route by route ID and by the agency as a whole. 
def trip_df():
    
    # Read in file
    trips_file = "gs://calitp-analytics-data/data-analyses/rt_delay/compiled_cached_views/trips_2022-09-14_all.parquet"
    df = pd.read_parquet(trips_file)

    # Standardize route id
    df["route_id"] = df["route_id"].str.lower().str.strip()

    # Aggregate trips_df: aggregate trip_id by ITP ID and Route ID
    df2 = (
        df.groupby(["calitp_itp_id", "route_id"])
        .agg({"trip_id": "nunique"})
        .reset_index()
        .rename(columns={"trip_id": "total_trips_by_route"})
    )
    # Aggregate trips_df: count number of trips an agency makes
    # across all routes
    df3 = (
        df.groupby(["calitp_itp_id"])
        .agg({"trip_id": "nunique"})
        .reset_index()
        .rename(columns={"trip_id": "total_trips_by_agency"})
    )

    # Merge to get one comprehensive df
    m1 = pd.merge(df2, df3, how="inner", on="calitp_itp_id")

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
    "gs://calitp-analytics-data/data-analyses/5311 /2020-Vehicles_1.xlsm",
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
 
"""
Analysis Functions
"""
# Overlay Federal Communications Commission map with original bus routes df.
def comparison(gdf_left, gdf_right):

    # Overlay
    overlay_df = gpd.overlay(
        gdf_left, gdf_right, how="intersection", keep_geom_type=False
    )

    # Create a new route length for portions covered by cell coverage
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
    overlay =  comparison(original_routes_df, provider_gdf)

    # Aggregate lengths of routes by route id, name, agency, and itp_id
    # 10/18: removed route name
    overlay2 = (overlay.dissolve(
         by=["route_id","agency", "itp_id"],
         aggfunc={
         "route_length": "sum"}).reset_index()) 

    # Merge original dataframe with old route with provider-route overlay
    # To compare original route length and old route length
    # 10/18: removed route name
    m1 = overlay2.merge(
        original_routes_df,
        how="inner",
        on=["route_id","agency", "itp_id"],
        suffixes=["_overlay", "_original_df"],
    )
    
    
    # Create % of route covered by data vs. not 
    m1["percentage"] = (
        m1["route_length_overlay"] / m1["route_length_original_df"]
    ) * 100
    
    # Create bins  
    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    m1["binned"] = pd.cut(m1["percentage"], bins)
    
    # Drop unwanted cols 
    unwanted_cols = ["route_type", "geometry_original_df"]
    m1 = m1.drop(columns = unwanted_cols)
    
    # Sort
    m1 = m1.sort_values(["route_id","route_name", "agency"]) 
    
    # Add suffix to certain columns to distinguish which provider 
    # https://stackoverflow.com/questions/53380310/how-to-add-suffix-to-column-names-except-some-columns
    m1 = m1.rename(columns={c: c+ suffix for c in m1.columns if c in ['percentage', 'binned', 'route_length_overlay',
                                                                     'geometry_overlay']})

    # Ensure m1 is a GDF 
    m1 = gpd.GeoDataFrame(m1, geometry = f"geometry_overlay{suffix}", crs = "EPSG:4326")
    return m1

# Clip the provider map to a county and return the areas of a county
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