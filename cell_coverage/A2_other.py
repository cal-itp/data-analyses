from shared_utils import geography_utils
from shared_utils import utils
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

import A1_provider_prep

"""
Prepping Other Data Sources
(Routes/Trips/NTD)
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

"""
Get Unique Routes
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

def clip_route_district(district_df):
    """
    Find which routes fall 100% in one district and
    which cross district boundaries.
    """
    # Load unique routes & districts
    unique_routes = load_unique_routes_df()
    
    # Clip routes against a district
    clipped = gpd.clip(unique_routes, district_df)

    # Get route length after doing clip
    clipped = clipped.assign(
        clipped_route_length=clipped.geometry.to_crs(
            geography_utils.CA_StatePlane
        ).length
    )

    # Get %
    clipped["route_percentage"] = (
        (clipped["clipped_route_length"] / clipped["original_route_length"]) * 100
    ).astype("int64")

    return clipped

def complete_clip_route_district() -> dg.GeoDataFrame:
    """
    Find which routes are in only 1 or 1+ districts
    for each district. 
    """
    # Load districts
    district_df = A1_provider_prep.get_districts()

    full_gdf = pd.DataFrame()
    
    all_districts = [*range(1, 13, 1)]
    
    for i in all_districts:
        result = clip_route_district(district_df[district_df.district == i])
        # Column to indicate which district this route runs in
        result["District"] = f"D-{i}"
        
        full_gdf = dd.multi.concat([full_gdf, result], axis=0)

    full_gdf = full_gdf.compute()

    return full_gdf

def turn_counts_to_df(df, col_of_interest:str):
    """
    Takes a column, finds value counts,
    and turns the results into a dataframe.
    """
    df = (
    df[col_of_interest]
    .value_counts()
    .to_frame()
    .reset_index()
    .rename(columns = 
        { col_of_interest:'total',
         'index':col_of_interest})
    )
    return df

def aggregate_routes(gdf):
    # Return one row for each route
    dissolved = gdf.dissolve(
        by=["agency", "itp_id", "route_id", "long_route_name", "District"],
        aggfunc={
            "original_route_length": "max",
        },
    ).reset_index()

    return dissolved

def find_multi_district_routes():
    """
    Definitively summarize which routes are in one district versus 
    those that run in various districts. Returns
    a df with multi-district routes, a df with one-district
    routes, and the original clipped df.
    """
    # Clip the routes against districts
    clipped_df = complete_clip_route_district()
    
    # Get value counts for long route names to figure out which routes has 1+ row. 
    # if a route has 1+ row, that means its runs in 1+ district
    value_counts_df = turn_counts_to_df(clipped_df, 'long_route_name')
    
    # Filter for routes w/ 1+ row
    routes_in_multi_district = ((value_counts_df[value_counts_df.total > 1])[['long_route_name']]).reset_index(drop = True)
    
    # Place the values into a list
    routes_in_multi_districts_list = routes_in_multi_district.long_route_name.tolist()
    
    # Filter the original dataframe for routes in multiple districts
    routes_in_multi_district = (clipped_df[clipped_df.long_route_name.isin(routes_in_multi_districts_list)]).reset_index(drop = True)
    
    # Filter the original dataframe for routes in only one districts
    routes_in_one_district = (clipped_df[~clipped_df.long_route_name.isin(routes_in_multi_districts_list)]).reset_index(drop = True)
    
    # Because routes that run in multi districts are split in 1+ row, aggregate them back
    routes_in_multi_district = aggregate_routes(routes_in_multi_district)
    clipped_df = aggregate_routes(clipped_df)
    
    return routes_in_one_district, routes_in_multi_district, clipped_df

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
"""
agencies_dict = {
        "Trinity County": "Trinity Transit",
        "City of Calabasas": "Calabasas Transit System",
        "County of Sonoma": "Sonoma County Transit",
        "Tehama County": "Tehama Rural Area eXpress",
        "Los Angeles County Department of Public Works - East L.A.": "East Los Angeles Shuttle",
        "Sacramento Regional Transit District": "Sacramento Regional Transit District",
        "City of Lompoc": "City of Lompoc Transit",
        "San Luis Obispo Regional Transit Authority": "South County Transit Link",
        "City of Roseville": "Roseville Transit",
        "Los Angeles County Dept. of Public Works - Athens Shuttle Service": "the Link-Athens",
        "Los Angeles County Department of Public Works - Avocado Heights": "Avocado Heights/Bassett/West Valinda Shuttle",
        "Susanville Indian Rancheria": "Susanville Indian Rancheria Public Transportation Program",
        "Transit Joint Powers Authority for Merced County": "Merced The Bus",
        "City of Eureka": "Eureka Transit Service",
        "Nevada County Transit Services": "Gold Country Stage",
        "San Mateo County Transit District": "SamTrans",
        "Redwood Coast Transit Authority": "Redwood Coast Transit",
        "City of Avalon": "Avalon Transit",
        "City of Lodi": "Grapeline",
        "Golden Gate Bridge": "Golden Gate Bridge Highway and Transportation District",
        "City of Santa Maria": "Santa Maria Area Transit",
        'City and County of San Francisco': 'MUNI',
        'Alameda-Contra Costa Transit District': 'AC Transit',
        'Kern Regional Transit': 'Kern Transit',
        'County of Placer': 'Tahoe Transportation',
        'County of Placer':'Tahoe Truckee Area Regional Transportation',
        'City of Tulare':'Tulare County Regional Transit Agency'
    }

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
    
    # Replace some manually to match routes df
    df.agency = df.agency.replace(agencies_dict)
    
    # Add up buses
    df["total_buses"] = df.sum(numeric_only=True, axis=1)
    
    # Drop agencies with 0 buses
    df = df.loc[df['total_buses'] !=0]
    
    return df

"""
Add GTFS
"""
# Agencies that were left out
data = [
    [177, "the Link Florence-Firestone/Walnut Park", "GTFS but no additional details"],
    [181, "the Link Willowbrook", "GTFS but no additional details"],
    [176, "the Link-Athens", "GTFS but no additional details"],
    [179, "the Link Lennox", "GTFS but no additional details"],
    [178, "the Link King Medical Center", "GTFS but no additional details"],
]
    
# Downloaded from Airtable "Organizations"
# https://airtable.com/appPnJWrQ7ui4UmIl/tblFsd8D5oFRqep8Z/viwVBVSd0ZhYu8Ewm?blocks=hide
def load_gtfs(): 
 
    subset_gtfs = ["itp_id", "name", "gtfs_static_status", "gtfs_realtime_status"]
    
    df = to_snakecase(
    pd.read_csv(f"{A1_provider_prep.GCS_FILE_PATH}airtable_organizations.csv"))[subset_gtfs]
    
    df.itp_id = df.itp_id.fillna(0).astype(int)
    
    df.name = df.name.replace(
    {"Eastern Sierra Transit Authority": "Mammoth Lakes Transit System"})
    
    # Consolidate GTFS into one col
    df["gtfs_status"] = df.gtfs_static_status + '/' + df.gtfs_realtime_status
    
    # Drop old cols
    df = df.drop(columns = ["gtfs_static_status", "gtfs_realtime_status"])
    
    # Manually add some agencies that didn't show up
    additional_agencies = pd.DataFrame(data, columns=['itp_id', 'name', 'gtfs_status'])
    df = pd.concat([df, additional_agencies], ignore_index=True)
    
    return df 