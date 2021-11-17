"""
Utility functions for census tract data.
"""
import geopandas as gpd
import pandas as pd

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


def attach_tract_geometry(df, tract_geometry_df, 
                          merge_col = ["Tract"], join="left"):
    """
    df: pandas.DataFrame
        The df that needs tract geometry added.
    tract_geometry_df: geopandas.GeoDataFrame
        The gdf that supplies the tract polygon geometry.
    merge_col: list. 
        List of columns to do the merge on. 
    join: str.
        Specify whether it's a left, inner, or outer join.
        
    Returns a geopandas.GeoDataFrame
    """    
    gdf = pd.merge(
        tract_geometry_df.to_crs(WGS84),
        df,
        on = merge_col,
        how = join,
    )
    
    return gdf