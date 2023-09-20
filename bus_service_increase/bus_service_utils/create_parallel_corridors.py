"""
Utility functions for working with
State Highway Network and transit routes.

Highways Raw data: [SHN on Geoportal](https://opendata.arcgis.com/datasets/77f2d7ba94e040a78bfbe36feb6279da_0.geojson) 
Processed data: run raw data through `create_parallel_corridors.py` 
export to GCS, put in catalog.

Transit Routes: Need a shapes level table with route info.
Usually, this can be achieved by merging `trips` and `shapes`.
"""
import geopandas as gpd
import pandas as pd

from typing import Literal

from shared_utils import geography_utils, utils

DATA_PATH = "./data/"

#--------------------------------------------------------#
### Functions to create overlay dataset + further cleaning
#--------------------------------------------------------#
def process_transit_routes(
    df: gpd.GeoDataFrame, 
    warehouse_version: Literal["v1", "v2"]
) -> gpd.GeoDataFrame:
    """
    At operator level, pick the route with the longest length 
    to overlay with SHN.
    At operator level, sum up how many unique routes there are.
    """
    
    if warehouse_version == "v1":
        operator_cols = ["calitp_itp_id"]
        
    elif warehouse_version == "v2":
        operator_cols = ["feed_key"]
    
    ## Clean transit routes
    df = df.assign(
        route_length = df.to_crs(
            geography_utils.CA_StatePlane).geometry.length
    ).to_crs(geography_utils.CA_StatePlane)
    
    # Get it down to route_id and pick longest shape
    df2 = (df.sort_values(operator_cols + ["route_id", "route_length"], 
                          ascending = [True, True, False])
           .drop_duplicates(subset=operator_cols + ["route_id"])
           .reset_index(drop=True)
    )
    
    # Then count how many unique route_ids appear for operator (that's denominator)
    route_cols = ["route_id", "total_routes", "route_length", "geometry"]
    
    if warehouse_version == "v2":
        keep_cols = operator_cols + ["name"] + route_cols
    else:
        keep_cols = operator_cols + route_cols
    
    df3 = df2.assign(
        total_routes = df2.groupby(operator_cols)["route_id"].transform(
            "nunique").astype("Int64")
    )[keep_cols]
    
    return df3


def prep_highway_directions_for_dissolve(
    group_cols: list = ["Route", "County", "District", "RouteType"]
) -> gpd.GeoDataFrame:    
    '''
    Put in a list of group_cols, and aggregate highway segments with 
    the direction info up to the group_col level.
    '''
    df = (gpd.read_parquet("gs://calitp-analytics-data/data-analyses/"
                           "shared_data/state_highway_network.parquet")
          .to_crs(geography_utils.CA_StatePlane))
    
    # Get dummies for direction
    # Can make data wide instead of long
    direction_dummies = pd.get_dummies(df.Direction, dtype=int)
    df = pd.concat([df.drop(columns = "Direction"), 
                    direction_dummies], axis=1)

    direction_cols = ["NB", "SB", "EB", "WB"]
    
    for c in direction_cols:
        df[c] = df.groupby(group_cols)[c].transform('max').astype(int)

    return df


def process_highways(
    buffer_feet: int = geography_utils.FEET_PER_MI
) -> gpd.GeoDataFrame:
    """
    For each highway, store what directions it runs in 
    as dummy variables. This will allow us to dissolve 
    the geometry and get fewer rows for highways
    without losing direction info.
    """
    
    group_cols = ["Route", "County", "District", "RouteType"]
    df = prep_highway_directions_for_dissolve(group_cols)
    
    # Buffer first, then dissolve
    # If dissolve first, then buffer, kernel times out
    df = df.assign(
        highway_length = df.geometry.length,
        geometry = df.geometry.buffer(buffer_feet),
        Route = df.Route.astype(int),
    )
    
    direction_cols = ["NB", "SB", "EB", "WB"]
    df = df.dissolve(by=group_cols + direction_cols).reset_index()
    
    df[direction_cols] = df[direction_cols].astype(int)
        
    return df


def overlay_transit_to_highways(
    hwy_buffer_feet: int = geography_utils.FEET_PER_MI,
    transit_routes_df: gpd.GeoDataFrame = None, 
    warehouse_version: Literal["v1", "v2"] = "v2"
) -> gpd.GeoDataFrame:
    """
    Function to find areas of intersection between
    highways (default of 1 mile buffer) and transit routes.
    
    Returns: geopandas.GeoDataFrame, with geometry column reflecting
    the areas of intersection.
    """
    
    # Can pass a different buffer zone to determine parallel corridors
    highways = process_highways(buffer_feet = hwy_buffer_feet)
    transit_routes = process_transit_routes(transit_routes_df, warehouse_version)
    
    # Overlay
    # Note: an overlay based on intersection changes the geometry column
    # The new geometry column will reflect that area of intersection
    gdf = gpd.overlay(
        transit_routes, 
        highways, 
        how = "intersection", 
        keep_geom_type = True
    )  
    
    # Using new geometry column, calculate what % that intersection 
    # is of the route and hwy
    gdf = gdf.assign(
        pct_route = (gdf.geometry.length / gdf.route_length).round(3),
        pct_highway = (gdf.geometry.length / gdf.highway_length).round(3),
        Route = gdf.Route.astype(int),
    )
    
    '''
    Set pct_highway to have max of 1
    LA Metro Line 910 (Silver Line) runs on the 110 freeway in both directions.
    Has to do with the fact that highway length was calculated only 
    for 1 direction (~centerline). 
    The length was calculated, 1 mi buffer drawn, then the 
    dissolve (computationally expensive).
    '''

    gdf = gdf.assign(
        pct_highway = gdf.apply(lambda x: 1 if x.pct_highway > 1 
                                else x.pct_highway, axis=1),
    )
    
    # Fix geometry - don't want the overlay geometry, which is intersection
    # Want to use a transit route's line geometry
    if warehouse_version == "v1":
        operator_cols = ["calitp_itp_id"]
        
    elif warehouse_version == "v2":
        operator_cols = ["feed_key", "name"]
    
    gdf2 = pd.merge(
        transit_routes[operator_cols + ["route_id", "geometry"]
                      ].drop_duplicates(),
        gdf.drop(columns = "geometry"),
        on = operator_cols + ["route_id"],
        how = "left",
        # Allow 1:m merge because the same transit route 
        # can overlap with various highways
        validate = "1:m",
        indicator=True
    )
    
    return gdf2


def parallel_or_intersecting(
    df: gpd.GeoDataFrame, 
    pct_route_threshold: float = 0, 
    pct_highway_threshold: float = 0
) -> gpd.GeoDataFrame:
    
    # Play with various thresholds to decide how to designate parallel
    df = df.assign(
        parallel = df.apply(lambda x: 
                            1 if (
                                (x.pct_route >= pct_route_threshold) and 
                                (x.pct_highway >= pct_highway_threshold)
                            ) else 0, axis=1),
    )
    
    return df


# Use this in notebook
# Can pass different parameters if buffer or thresholds need adjusting
def make_analysis_data(
    hwy_buffer_feet:int = geography_utils.FEET_PER_MI,
    transit_routes_df: gpd.GeoDataFrame = None,
    pct_route_threshold: float = 0.5,
    pct_highway_threshold: float = 0.1,
    data_path: str = "", 
    file_name: str = "parallel_or_intersecting",
    warehouse_version: Literal["v1", "v2"] = "v2"
):
    '''
    hwy_buffer_feet: int, number of feet to draw hwy buffers, defaults to 1 mile
    transit_routes_df: a table, typically shapes, with route_id too
    pct_route_threshold: float, between 0-1
    pct_highway_threshold: float, between 0-1
    data_path: str, file path for saving parallel transit routes dataset
    file_name: str, file name for saving parallel transit routes dataset
    
    '''
    # Get overlay between highway and transit routes
    # Draw buffer around highways
    gdf = overlay_transit_to_highways(
        hwy_buffer_feet, transit_routes_df, warehouse_version)

    # Remove transit routes that do not overlap with highways
    gdf = (gdf[gdf._merge!="left_only"]
           .drop(columns = "_merge")
           .reset_index(drop=True)
          )
    
    # Categorize whether route is parallel or intersecting based on threshold
    gdf2 = parallel_or_intersecting(
        gdf, 
        pct_route_threshold, 
        pct_highway_threshold
    )
    
    if "gs://" in data_path:
        utils.geoparquet_gcs_export(
            gdf2, 
            data_path, 
            file_name
        )
    else:
        gdf2.to_parquet(f"{data_path}{file_name}.parquet")
        
    # For map, need highway to be 250 ft buffer
    #highways = process_highways(buffer_feet=250)
    #highways.to_parquet(f"{DATA_PATH}highways.parquet")