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
    
    overlay_df = overlay_df.drop_duplicates().reset_index(drop = True)
    # utils.geoparquet_gcs_export(overlay_df, GCS_FILE_PATH, f"{provider_name}_overlaid_with_unique_routes")
    return overlay_df

def overlay_single_routes(
    provider: gpd.GeoDataFrame, routes: gpd.GeoDataFrame, provider_name: str
):
    """
    For any provider or routes that may cause an issue,
    use this function to pass and print out the names of
    problematic routes.
    """

    # Empty dataframe to hold results
    all_intersected_routes = pd.DataFrame()

    # List of long route names
    unique_routes_list = routes.long_route_name.unique().tolist()

    # Intersect route by route, skipping those that don't work
    for i in unique_routes_list:
        filtered = routes[routes.long_route_name == i].reset_index(drop=True)
        try:
            intersected = gpd.overlay(
                provider, filtered, how="intersection", keep_geom_type=False
            )
            all_intersected_routes = pd.concat(
                [all_intersected_routes, intersected], axis=0
            )
        except:
            pass
            print(f"{i}")

    utils.geoparquet_gcs_export(
        all_intersected_routes,
        utilities.GCS_FILE_PATH,
        f"{provider_name}_overlaid_with_unique_routes",
    )

    return all_intersected_routes

def dissolve_summarize(provider: gpd.GeoDataFrame):
    """
    Drop duplicate rows of the provider map overlaid
    with unique routes. Aggregate so there's only
    one row for each route-agency-route ID combo. 
    Find % of new route length compared with original 
    route length.
    """
    provider = provider.dissolve(
         by=["agency","long_route_name"],
         aggfunc={
         "route_length": "sum", "original_route_length":"max"}).reset_index()
    
    provider["percentage_route_covered"] = ((provider["route_length"] / provider["original_route_length"])* 100).astype('int64')
    
    return provider

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