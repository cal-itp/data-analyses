import A1_provider_prep
import A2_other

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

# Storage
from calitp.storage import get_fs
fs = get_fs()
import os

import datetime
from loguru import logger

"""
Overlay Routes against Provider Maps
"""
def routes_1_dist_comparison(routes_gdf, provider_gdf, suffix:str):
    """
    Overlay routes that run in only one district against 
    the provider map that has no coverage to find %
    of route that run in areas without coverage. 
    
    routes_gdf: routes that run in 1 district.
    provider_gdf: the provider map.
    suffix: provider name, suffix to distinguish between 
    the different provider gdfs. 
    """
    start = datetime.datetime.now()
    
    # Overlay
    overlay_df = gpd.overlay(
        routes_gdf, provider_gdf, how="intersection", keep_geom_type=False
    )

    # Create a new route length for portions covered by cell coverage
    overlay_df = overlay_df.assign(
        no_coverage_route_length=overlay_df.geometry.to_crs(geography_utils.CA_StatePlane).length
    )
    
    overlay_df = overlay_df.drop_duplicates().reset_index(drop = True)
   
   # Dissolve to make sure each route only belongs to one line.
    overlay_df = overlay_df.dissolve(
         by=["agency","itp_id", "route_id", "long_route_name", "District"],
         aggfunc={
         "no_coverage_route_length": "sum", "original_route_length":"max"}).reset_index()
    
    # Find percentage of route enters a no coverage zone. 
    overlay_df["percentage_of_route_wo_coverage"] = ((overlay_df["no_coverage_route_length"] /   overlay_df["original_route_length"])* 100).astype('int64')
    
    overlay_df = overlay_df.rename(columns={c: c+ suffix for c in overlay_df.columns if c in ['percentage_of_route_wo_coverage', 'original_route_length', 'no_coverage_route_length']})
    
    end = datetime.datetime.now()
        
    logger.info(f"execution time: {end-start}")
    
    utils.geoparquet_gcs_export(overlay_df, A1_provider_prep.GCS_FILE_PATH, f"{suffix}_overlay_routes_in_1_dist")
    return overlay_df

def group_multi_dist_routes(df):
    df = df.dissolve(by=["agency","itp_id","route_id","long_route_name"],
         aggfunc={
         "no_coverage_route_length": "sum",
         "original_route_length": "max"}).reset_index()
    
    return df

def summarize_rows(df, col_to_group: list, col_to_summarize: str):
    df_col_to_summarize = (
        df.groupby(col_to_group)[col_to_summarize].apply(",".join).reset_index()
    )
    return df_col_to_summarize

def multi_dist_route_comparison(routes_gdf, provider_gdf, suffix:str):
    """
    Overlay routes that run in 1+ district against 
    the provider map that has no coverage. 
    
    routes_gdf: routes that run in 1+ districts.
    provider_gdf: the provider map.
    suffix: provider name, suffix to distinguish between 
    the different provider gdfs. 
    """
    start = datetime.datetime.now()
    
    # Overlay
    multi_route = gpd.overlay(
        routes_gdf, provider_gdf, how="intersection", keep_geom_type=False
    )
    
    # Get length of new geometry after overlaid
    multi_route = multi_route.assign(
        no_coverage_route_length=multi_route.geometry.to_crs(geography_utils.CA_StatePlane).length
    )
    
    # Group the routes and sum up the areas without coverage, regardless of district
    multi_route_grouped = group_multi_dist_routes(multi_route)
    
    # The district information of each route is on a different line. Group them onto the same.
    multi_route_summed = summarize_rows(multi_route, ["long_route_name"], "District")
    
    # Merge these two 
    merge1 = multi_route_grouped.merge(multi_route_summed, how="inner", on="long_route_name")
    
    # Calculate % of route that crosses a no-data coverage zone. 
    merge1["percentage_of_route_wo_coverage"] = ((merge1["no_coverage_route_length"] / merge1["original_route_length"])* 100).astype('int64')
    
    # Add a suffix. 
    merge1 = merge1.rename(columns={c: c+ suffix for c in merge1.columns if c in ['percentage_of_route_wo_coverage', 'original_route_length', 'no_coverage_route_length']})
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    utils.geoparquet_gcs_export(merge1, A1_provider_prep.GCS_FILE_PATH, f"{suffix}_overlay_routes_in_multi_dist")
    
    return merge1

def stack_all_routes(provider_gdf, provider: str):
    """
    Stack the routes that run in multiple districts and one 
    district into one gdf by provider.
    
    provider_gdf: the file created by `find_difference_gdf` and 
    `stack_all_maps` in A1_provider_prep
    
    provider: provider name for file naming
    """
    start = datetime.datetime.now()
    
    # Grab all routes
    one_dist_routes, multi_dist_routes, all_routes = A2_other.find_multi_district_routes()
    
    # Run the routes
    multi_dist_o =  multi_dist_route_comparison(multi_dist_routes, provider_gdf, provider)
    one_dist_o =  routes_1_dist_comparison(one_dist_routes, provider_gdf, provider)
    
    # Concat the routes
    df_list = [multi_dist_o, one_dist_o]

    # Concat files
    stacked_df = dd.multi.concat(df_list)
    stacked_df = stacked_df.compute()
    
    utils.geoparquet_gcs_export(stacked_df, A1_provider_prep.GCS_FILE_PATH, f"{provider}_overlaid_all_routes")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
        
    return stacked_df

subset_cols = ["agency", 'itp_id', 'route_id', "long_route_name", "District", "median_percent_with_coverage", "median_percent_no_coverage", "geometry_x","percentage_of_route_wo_coverage_att", "percentage_of_route_wo_coverage_verizon",  "percentage_of_route_wo_coverage_tmobile"]

def merge_all_providers():
    """
    Merge all the overlaid unique routes among all 
    3 providers into one large dataframe. Use an inner
    merge, since those are routes that
    touch a no data zone for every provider.
    """
    # Read files
    verizon_o = gpd.read_parquet(f"{A1_provider_prep.GCS_FILE_PATH}_verizon_overlaid_all_routes.parquet")
    att_o = gpd.read_parquet(f"{A1_provider_prep.GCS_FILE_PATH}_att_overlaid_all_routes.parquet")
    tmobile_o = gpd.read_parquet(f"{A1_provider_prep.GCS_FILE_PATH}_tmobile_overlaid_all_routes.parquet")
    
    # Merge on the common cols
    common_cols = ['agency', 'itp_id', 'route_id', 'long_route_name', 'District',]
    
    # Inner merge - or outer?
    m1 = verizon_o.merge(att_o, how="inner", on=common_cols).merge(
    tmobile_o, how="inner", on=common_cols)
    
    # Grab percentages to calculate median percentage
    # w/o coverage across ATT, Tmobile, and Verizon
    percentage_cols = ["percentage_of_route_wo_coverage_att", "percentage_of_route_wo_coverage_verizon",  "percentage_of_route_wo_coverage_tmobile"]
    
    # Fill NA
    m1[percentage_cols] = m1[percentage_cols].fillna(0)
    
    m1["median_percent_no_coverage"] = m1[percentage_cols].median(axis=1)
    
    # Subtract by 100 for percent of route w/ coverage
    m1["median_percent_with_coverage"] = 100-m1["median_percent_no_coverage"]
    
    # Set geometry 
    m1 = m1.set_geometry("geometry_x")
    
    m1 = m1[subset_cols]
    
    return m1

"""
Final Dataframe
merges all 4 data sources
"""
def merge_trips(routes_gdf):
    """
    Find # of trips an agency ran for a particular 
    route by Cal ITP ID & Route ID. Find # of trips
    an agency ran across all routes for that day.
    """
    # Load in trips 
    trips = A2_other.trip_df()
    
    # Merge on ITP ID and Route ID
    m1 = pd.merge(
    routes_gdf,
    trips,
    how="left",
    left_on=["itp_id", "route_id"],
    right_on=["calitp_itp_id", "route_id"])
    
    # If total trips by route/agency are NA for total trips, fill with median of each col
    m1 = m1.assign(
     total_trips_by_route = m1.total_trips_by_route.fillna(m1.total_trips_by_route.median()),
     total_trips_by_agency = m1.total_trips_by_agency.fillna(m1.total_trips_by_agency.median())) 
    
    # Divide the total of trips for this particular low coverage route
    # by the total trips the agency run among all its routes on that particular day
    m1["percentage_of_trips_w_low_cell_service"] = (
    m1["total_trips_by_route"] / m1["total_trips_by_agency"])
    
    return m1

def merge_ntd(gdf):
    """
    Add in NTD information to find number of buses
    """
    # Load NTD vehicles data. 
    ntd = A2_other.ntd_vehicles()[["agency", "total_buses"]]
    
    # Replace so it will merge properly with NTD
    gdf["agency"] = gdf["agency"].replace(
    {"Mammoth Lakes Transit System": "Eastern Sierra Transit Authority"})
    
    # Merge
    m1 = pd.merge(
    gdf,
    ntd,
    how="left",
    on="agency",
    indicator=True,)
    
    # Fill agencies with NA buses with median total buses
    median_total_buses = m1["total_buses"].median()
    m1["total_buses"] = m1["total_buses"].fillna(median_total_buses)
    
    # To get an estimate of buses that run in a low data zone.
    # Multiply the agency's total buses by the % of its total trips that 
    # run a "low data coverage route." 
    m1["estimate_of_buses_in_low_cell_zones"] = (m1.total_buses * m1.percentage_of_trips_w_low_cell_service).astype('int64')
    
    # Replace estimate of buses from 0 to 1.
    # Since at least 1 bus will run on that route.
    m1.estimate_of_buses_in_low_cell_zones = m1.estimate_of_buses_in_low_cell_zones.replace({0:1})
    
    return m1

def final_merge(routes_gdf):
    """
    Merge provider maps, NTD, routes, and
    trips all together. 
    """
    # Merge with trips df to get # of trips
    # run for each route by route ID-Cal ITP
    m1 = merge_trips(routes_gdf)
    
    # Merge with NTD df to get # of buses
    m2 = merge_ntd(m1)
    
    # Drop columns
    cols_to_drop = ['itp_id', 'route_id', '_merge', 'calitp_itp_id']
    m2 = m2.drop(columns = cols_to_drop)
    
    # Clean up columns
    m2.columns = m2.columns.str.replace("_", " ").str.strip().str.title()
    
    # Clean up % values
    m2["Percentage Of Trips W Low Cell Service"] = (m2["Percentage Of Trips W Low Cell Service"] * 100)
    
    # Ensure this remains a GDF
    m2 = m2.rename(columns =  {"Geometry X":"Geometry"})
    m2 = m2.set_geometry("Geometry")
    

    return m2

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
OLD
"""
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

    #utils.geoparquet_gcs_export(
    #    all_intersected_routes,
     #   utilities.GCS_FILE_PATH,
      #  f"{provider_name}_overlaid_with_unique_routes",
    #)

    return all_intersected_routes