"""
Utility functions for census tract data.
"""
import geopandas as gpd
import pandas as pd

import utils
import prep_data


def aggregate_by_tract(gdf, group_cols, 
                       sum_cols = [], count_cols = [], nunique_cols = []):
    '''
    gdf: geopandas.GeoDataFrame, on which the aggregating to tract is done
        It must include the tract's geometry column
    
    group_cols: list. List of columns to do the groupby, but exclude geometry.
    sum_cols: list. List of columns to calculate a sum with the groupby.
    count_cols: list. List of columns to calculate a count with the groupby.
    
    Returns a geopandas.GeoDataFrame.
    '''
    df2 = gdf[group_cols].drop_duplicates().reset_index()
    
    if len(sum_cols) > 0:
        sum_df = gdf.pivot_table(index=group_cols, 
                                values=sum_cols, 
                                aggfunc="sum").reset_index()
        df2 = pd.merge(df2, sum_df,
                      on=group_cols, how="left", validate="1:1"
                     )
  
    if len(count_cols) > 0:
        count_df = gdf.pivot_table(index=group_cols, 
                                  values=count_cols, 
                                  aggfunc="count").reset_index()
        df2 = pd.merge(df2, count_df, 
                     on=group_cols, how="left", validate="1:1")
    
    if len(nunique_cols) > 0:
        nunique_df = gdf.pivot_table(index=group_cols,
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
        tract_geometry_df.to_crs(utils.WGS84),
        df,
        on = merge_col,
        how = join,
    )
    
    return gdf