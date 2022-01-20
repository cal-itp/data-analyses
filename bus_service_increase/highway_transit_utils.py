"""
Utility functions for working with
State Highway Network and transit routes.
"""
import geopandas as gpd
import intake
import pandas as pd

import shared_utils
import utils

catalog = intake.open_catalog("*.yml")

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
    

def process_transit_routes():
    ## Clean transit routes
    df = (catalog.transit_routes.read()
          .to_crs(shared_utils.geography_utils.CA_StatePlane))
    
    # Transit routes included some extra info about 
    # route_long_name, route_short_name, agency_id
    # Get it down to unique shape_id
    subset_cols = ["itp_id", "shape_id", "route_id", "geometry"]
    df = (df[subset_cols]
          .drop_duplicates()
          .reset_index(drop=True)
          .assign(route_length = df.geometry.length)
         )
    
    df = df.assign(
        total_routes = df.groupby("itp_id")["route_id"].transform("count")
    )
    
    return df


def process_highways(buffer_feet = shared_utils.geography_utils.FEET_PER_MI):
    df = (catalog.state_highway_network.read()
          .to_crs(shared_utils.geography_utils.CA_StatePlane))
    
    # Get dummies for direction
    # Can make data wide instead of long
    direction_dummies = pd.get_dummies(df.Direction, dtype=int)
    df = pd.concat([df.drop(columns = "Direction"), 
                    direction_dummies], axis=1)

    group_cols = ['Route', 'County', 'District', 'RouteType']
    direction_cols = ["NB", "SB", "EB", "WB"]
    
    for c in direction_cols:
        df[c] = df.groupby(group_cols)[c].transform('max')
    
    # Buffer first, then dissolve
    # If dissolve first, then buffer, kernel times out
    df = df.assign(
        highway_length = df.geometry.length,
        geometry = df.geometry.buffer(buffer_feet)   
    )
    
    df = df.dissolve(by=group_cols + direction_cols).reset_index()
    
    return df
    
    
def overlay_transit_to_highways():
    """
    Function to find areas of intersection between
    highways (default of 1 mile buffer) and transit routes.
    
    Returns: geopandas.GeoDataFrame, with geometry column reflecting
    the areas of intersection.
    """
    
    highways = process_highways(buffer_feet = 
                                shared_utils.geography_utils.FEET_PER_MI)
    transit_routes = process_transit_routes()
    
    # Overlay
    # Note: an overlay based on intersection changes the geometry column
    # The new geometry column will reflect that area of intersection
    gdf = gpd.overlay(transit_routes, highways, how = "intersection")

    return gdf

