"""
Utility functions for geospatial data.
Some functions for dealing with census tract or other geographic unit dfs.
"""
import geopandas as gpd
import os
import pandas as pd
import shapely

os.environ["CALITP_BQ_MAX_BYTES"] = str(50_000_000_000)

import calitp
from calitp.tables import tbl
from siuba import *

WGS84 = "EPSG:4326"
CA_StatePlane = "EPSG:2229" # units are in feet
CA_NAD83Albers = "EPSG:3310" # units are in meters

SQ_MI_PER_SQ_M = 3.86 * 10**-7

def aggregate_by_geography(df, group_cols, 
                       sum_cols = [], mean_cols = [], 
                       count_cols = [], nunique_cols = []):
    '''
    df: pandas.DataFrame or geopandas.GeoDataFrame., 
        The df on which the aggregating is done.
        If it's a geodataframe, it must exclude the tract's geometry column
    
    group_cols: list. 
        List of columns to do the groupby, but exclude geometry.
    sum_cols: list. 
        List of columns to calculate a sum with the groupby.
    mean_cols: list. 
        List of columns to calculate an average with the groupby 
        (beware: may want weighted averages and not simple average!!).
    count_cols: list. 
        List of columns to calculate a count with the groupby.
    nunique_cols: list. 
        List of columns to calculate the number of unique values with the groupby.
    
    Returns a pandas.DataFrame or geopandas.GeoDataFrame (same as input).
    '''
    df2 = df[group_cols].drop_duplicates().reset_index()
    
    if len(sum_cols) > 0:
        sum_df = df.pivot_table(index=group_cols, 
                                values=sum_cols, 
                                aggfunc="sum").reset_index()
        df2 = pd.merge(df2, sum_df,
                      on=group_cols, how="left", validate="1:1"
                     )
  
    if len(mean_cols) > 0:
        mean_df = df.pivot_table(index=group_cols, 
                                values=mean_cols, 
                                aggfunc="mean").reset_index()
        df2 = pd.merge(df2, mean_df,
                      on=group_cols, how="left", validate="1:1"
                     )
        
    if len(count_cols) > 0:
        count_df = df.pivot_table(index=group_cols, 
                                  values=count_cols, 
                                  aggfunc="count").reset_index()
        df2 = pd.merge(df2, count_df, 
                     on=group_cols, how="left", validate="1:1")
    
    if len(nunique_cols) > 0:
        nunique_df = df.pivot_table(index=group_cols,
                                     values=nunique_cols,
                                     aggfunc="nunique").reset_index()
        df2 = pd.merge(df2, nunique_df, 
                       on=group_cols, how="left", validate="1:1")
        
     
    return df2.drop(columns = "index")


def attach_geometry(df, geometry_df, 
                    merge_col = ["Tract"], join="left"):
    """
    df: pandas.DataFrame
        The df that needs tract geometry added.
    geometry_df: geopandas.GeoDataFrame
        The gdf that supplies the geometry.
    merge_col: list. 
        List of columns to do the merge on. 
    join: str.
        Specify whether it's a left, inner, or outer join.
        
    Returns a geopandas.GeoDataFrame
    """    
    gdf = pd.merge(
        geometry_df.to_crs(WGS84),
        df,
        on = merge_col,
        how = join,
    )
    
    return gdf


# Function to take transit stop point data and create lines 
def make_routes_shapefile(ITP_ID_LIST = [], CRS="EPSG:4326"):
    """
    Parameters:
    ITP_ID_LIST: list. List of ITP IDs found in agencies.yml
    CRS: str. Default is WGS84, but able to re-project to another CRS.
    
    Returns a geopandas.GeoDataFrame, where each line is the operator-route-line geometry.
    """
    all_routes = gpd.GeoDataFrame()
    
    for itp_id in ITP_ID_LIST:
        shapes = (tbl.gtfs_schedule.shapes()
                  >> filter(_.calitp_itp_id == int(itp_id))
                  >> collect()
        )

        # Make a gdf
        shapes = (gpd.GeoDataFrame(shapes, 
                              geometry = gpd.points_from_xy
                              (shapes.shape_pt_lon, shapes.shape_pt_lat),
                              crs = WGS84)
             )
                
        # Now, combine all the stops by stop sequence, and create linestring
        for route in shapes.shape_id.unique():
            single_shape = (shapes
                            >> filter(_.shape_id == route)
                            >> mutate(shape_pt_sequence = _.shape_pt_sequence.astype(int))
                            # arrange in the order of stop sequence
                            >> arrange(_.shape_pt_sequence)
            )
            
            # Convert from a bunch of points to a line (for a route, there are multiple points)
            route_line = shapely.geometry.LineString(list(single_shape['geometry']))
            single_route = (single_shape
                           [['calitp_itp_id', 'shape_id', 'calitp_extracted_at']]
                           .iloc[[0]]
                          ) ##preserve info cols
            single_route['geometry'] = route_line
            single_route = gpd.GeoDataFrame(single_route, crs=WGS84)
            
            all_routes = all_routes.append(single_route)
    
    all_routes = (all_routes.to_crs(CRS)
                  .sort_values(["calitp_itp_id", "shape_id"])
                  .reset_index(drop=True)
                 )
    
    return all_routes