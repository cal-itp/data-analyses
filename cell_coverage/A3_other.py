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
Unique Routes
"""
# traffic_ops/export/ca_transit_routes_[date].parquet
routes_file =  "gs://calitp-analytics-data/data-analyses/traffic_ops/export/ca_transit_routes_2022-09-14.parquet"

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

    
# Open routes file and find unique routes
def load_unique_routes_df():
    
    df = gpd.read_parquet(routes_file)
    
    # Find unique routes
    df =  unique_routes(df)
    
    # Standardize route id  
    df["route_id"] = df["route_id"].str.lower().str.strip()
    
    # B/c route names and route ids can be the same across different agencies,
    # Add these 3 different columns so the route will have a unique identifier.
    df['long_route_name'] = (df['route_name'] + ' ' + df['route_id'] + ' '  + ' ' + df['agency'])
    return df


"""
# of Trips
""" 
# File for trips
trips_file = "gs://calitp-analytics-data/data-analyses/rt_delay/compiled_cached_views/trips_2022-09-14_all.parquet"

# Find number of trips ran per route by route ID and by the agency as a whole. 
def trip_df():
    
    # Read in file
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
NTD Data
Answer how many buses does an agency owns?
"""
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