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
    

def overlay_transit_to_highways(buffer_feet = 
                                shared_utils.geography_utils.FEET_PER_MI):
    """
    Function to find areas of intersection between
    highways (default of 1 mile buffer) and transit routes.
    
    Returns: geopandas.GeoDataFrame, with geometry column reflecting
    the areas of intersection.
    """
    highways = (catalog.state_highway_network.read()
                .to_crs(shared_utils.geography_utils.CA_StatePlane))
    
    # Get dummies for direction
    # Can make data wide instead of long
    direction_dummies = pd.get_dummies(highways.Direction, dtype=int)
    highways = pd.concat([highways.drop(columns = "Direction"), 
                          direction_dummies], axis=1)


    group_cols = ['Route', 'County', 'District', 'RouteType']
    direction_cols = ["NB", "SB", "EB", "WB"]
    for c in direction_cols:
        highways[c] = highways.groupby(group_cols)[c].transform('max')
    
    # Buffer first, then dissolve
    # If dissolve first, then buffer, kernel times out
    highways = highways.assign(
                geometry = highways.geometry.buffer(buffer_feet)   
    )
    
    highways = highways.dissolve(by=group_cols + direction_cols).reset_index()
    
    ## Clean transit routes
    transit_routes = (catalog.transit_routes.read()
                      .to_crs(shared_utils.geography_utils.CA_StatePlane))
    
    # Transit routes included some extra info about 
    # route_long_name, route_short_name, agency_id
    # Get it down to unique shape_id
    subset_cols = ["itp_id", "shape_id", "route_id", "geometry"]
    transit_routes = (transit_routes[subset_cols]
                      .drop_duplicates()
                      .reset_index(drop=True)
                      .assign(route_length = transit_routes.geometry.length)
                     )
    
    # Overlay
    # Note: an overlay based on intersection changes the geometry column
    # The new geometry column will reflect that area of intersection
    gdf = gpd.overlay(transit_routes, highways, how = "intersection")

    return gdf

