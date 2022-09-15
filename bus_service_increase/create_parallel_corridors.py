"""
Utility functions for working with
State Highway Network and transit routes.

Highways Raw data: [SHN on Geoportal](https://opendata.arcgis.com/datasets/77f2d7ba94e040a78bfbe36feb6279da_0.geojson) 
Processed data: run raw data through `create_parallel_corridors.py` 
export to GCS, put in catalog.

Transit Routes: Use `traffic_ops/export_shapefiles.py` that 
creates `routes_assembled.parquet` in GCS, put in catalog.

Instead of catalog[file_name].read(), put in GCS path
because it's easier to import once these utility functions need to 
be importable across directories.
"""
import geopandas as gpd
import pandas as pd

import shared_utils
from bus_service_utils import utils


DATA_PATH = "./data/"
IMG_PATH = "./img/"

#--------------------------------------------------------#
### State Highway Network
#--------------------------------------------------------#
def clean_highways():
    HIGHWAY_URL = ("https://opendata.arcgis.com/datasets/"
                   "77f2d7ba94e040a78bfbe36feb6279da_0.geojson")
    gdf = gpd.read_file(HIGHWAY_URL)
    
    keep_cols = ['Route', 'County', 'District', 'RouteType',
                 'Direction', 'geometry']
    
    gdf = gdf[keep_cols]
    print(f"# rows before dissolve: {len(gdf)}")
    
    # See if we can dissolve further - use all cols except geometry
    # Should we dissolve further and use even longer lines?
    dissolve_cols = list(gdf.columns)
    dissolve_cols.remove('geometry')
    
    gdf2 = gdf.dissolve(by=dissolve_cols).reset_index()
    print(f"# rows after dissolve: {len(gdf2)}")
    
    # Export to GCS
    shared_utils.utils.geoparquet_gcs_export(gdf2,
                                         utils.GCS_FILE_PATH, 
                                         "state_highway_network")
    
    
#--------------------------------------------------------#
### Functions to create overlay dataset + further cleaning
#--------------------------------------------------------#
# Can this function be reworked to take a df?
def process_transit_routes(alternate_df: 
                           gpd.GeoDataFrame = None) -> gpd.GeoDataFrame:
    if alternate_df is None:
        df = gpd.read_parquet(
            f"{utils.GCS_FILE_PATH}2022_Jan/shapes_processed.parquet")
    else:
        df = alternate_df
    
    ## Clean transit routes
    df = df.to_crs(shared_utils.geography_utils.CA_StatePlane)

    # Transit routes included some extra info about 
    # route_long_name, route_short_name, agency_id
    # Get it down to route_id instead of shape_id, pick longest shape
    df2 = df.assign(route_length = df.geometry.length)
    df3 = (df2.sort_values(["itp_id", "route_id", "route_length"], 
                  ascending=[True, True, False])
           .drop_duplicates(subset=["itp_id", "route_id"])
          .reset_index(drop=True)
         )
    
    # Then count how many unique route_ids appear for operator (that's denominator)
    keep_cols = [
        "itp_id", "route_id", "total_routes",
        "route_length", "geometry"
    ]
    
    df4 = df3.assign(
        total_routes = df3.groupby("itp_id")["route_id"].transform(
            "nunique").astype("Int64")
    )[keep_cols]
    
    return df4


def prep_highway_directions_for_dissolve(
    group_cols: list = ["Route", "County", "District", "RouteType"]
) -> gpd.GeoDataFrame:    
    '''
    Put in a list of group_cols, and aggregate highway segments with 
    the direction info up to the group_col level.
    '''
    df = (gpd.read_parquet(f"{utils.GCS_FILE_PATH}state_highway_network.parquet")
          .to_crs(shared_utils.geography_utils.CA_StatePlane))
    
    # Get dummies for direction
    # Can make data wide instead of long
    direction_dummies = pd.get_dummies(df.Direction, dtype=int)
    df = pd.concat([df.drop(columns = "Direction"), 
                    direction_dummies], axis=1)

    direction_cols = ["NB", "SB", "EB", "WB"]
    
    for c in direction_cols:
        df[c] = df.groupby(group_cols)[c].transform('max').astype(int)

    return df
    
    
def process_highways(buffer_feet: int = 
                     shared_utils.geography_utils.FEET_PER_MI
                    ) -> gpd.GeoDataFrame:
    
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
    hwy_buffer_feet: int = shared_utils.geography_utils.FEET_PER_MI,
    alternate_df = None
) -> gpd.GeoDataFrame:
    """
    Function to find areas of intersection between
    highways (default of 1 mile buffer) and transit routes.
    
    Returns: geopandas.GeoDataFrame, with geometry column reflecting
    the areas of intersection.
    """
    
    # Can pass a different buffer zone to determine parallel corridors
    highways = process_highways(buffer_feet = hwy_buffer_feet)
    transit_routes = process_transit_routes(alternate_df)
    
    # Overlay
    # Note: an overlay based on intersection changes the geometry column
    # The new geometry column will reflect that area of intersection
    gdf = gpd.overlay(transit_routes, 
                      highways, 
                      how = "intersection", 
                      keep_geom_type = False
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
    gdf2 = pd.merge(
        transit_routes[["itp_id", "route_id", "geometry"]].drop_duplicates(),
        gdf.drop(columns = "geometry"),
        on = ["itp_id", "route_id"],
        how = "left",
        # Allow 1:m merge because the same transit route can overlap with various highways
        validate = "1:m",
        indicator=True
    )
    
    return gdf2


def parallel_or_intersecting(df: gpd.GeoDataFrame, 
                             pct_route_threshold: float = 0.5, 
                             pct_highway_threshold: float = 0.1) -> gpd.GeoDataFrame:
    
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
def make_analysis_data(hwy_buffer_feet=
                       shared_utils.geography_utils.FEET_PER_MI,
                       alternate_df = None,
                       pct_route_threshold = 0.5,
                       pct_highway_threshold = 0.1,
                       DATA_PATH = "", FILE_NAME = "parallel_or_intersecting"
                      ):
    '''
    hwy_buffer_feet: int, number of feet to draw hwy buffers, defaults to 1 mile
    alternate_df: None or pandas.DataFrame
                    can input an alternate df for transit routes
                    otherwise, use the `shapes_processed` from other analysis
    pct_route_threshold: float, between 0-1
    pct_highway_threshold: float, between 0-1
    DATA_PATH: str, file path for saving parallel transit routes dataset
    FILE_NAME: str, file name for saving parallel transit routes dataset
    
    '''
    # Get overlay between highway and transit routes
    # Draw buffer around highways
    gdf = overlay_transit_to_highways(hwy_buffer_feet, alternate_df)

    # Remove transit routes that do not overlap with highways
    gdf = gdf[gdf._merge!="left_only"].drop(columns = "_merge").reset_index(drop=True)
    
    # Categorize whether route is parallel or intersecting based on threshold
    gdf2 = parallel_or_intersecting(gdf, 
                                    pct_route_threshold, 
                                    pct_highway_threshold)
    
    if "gs://" in DATA_PATH:
        shared_utils.utils.geoparquet_gcs_export(gdf2, DATA_PATH, FILE_NAME)
    else:
        gdf2.to_parquet(f"{DATA_PATH}{FILE_NAME}.parquet")
    
    # For map, need highway to be 250 ft buffer
    #highways = process_highways(buffer_feet=250)
    #highways.to_parquet(f"{DATA_PATH}highways.parquet")