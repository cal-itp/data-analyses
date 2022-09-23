"""
Functions for combining various tables 
from `gtfs_utils` queries. 
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from calitp.sql import to_snakecase

from shared_utils import geography_utils

def merge_routes_trips(
    routelines: gpd.GeoDataFrame | dg.GeoDataFrame, 
    trips: pd.DataFrame | dd.DataFrame,
    merge_cols: list = ["calitp_itp_id", "calitp_url_number", "shape_id"],
    crs: str = geography_utils.WGS84
) -> gpd.GeoDataFrame:
    """
    Merge routes (which has shape_id, geometry) with trips
    and returns a trips table with line geometry
    """

    routes = (routelines.drop_duplicates(subset=merge_cols)
              .reset_index(drop=True)
              [merge_cols + ["geometry"]]
             )
    
    if isinstance(routes, dg.GeoDataFrame):
        trips_with_geom = dd.merge(
            routes,
            trips,
            on = merge_cols,
            how = "left",
            indicator=True
        ).to_crs(crs).compute()  
        
    else:
        trips_with_geom = pd.merge(
            routes,            
            trips,
            on = merge_cols,
            how = "left",
            indicator=True
        ).to_crs(crs)
   
    return trips_with_geom


def aggregate_stat_by_group(
    df: dd.DataFrame,  
    group_cols: list, 
    stat_cols: dict = {"trip_id": "nunique", 
                       "departure_hour": "count",
                       "stop_id": "nunique"}
    ) -> pd.DataFrame:
    """
    Aggregate given different group_cols.
    """    
    
    def group_and_aggregate(df: dd.DataFrame, 
                            group_cols: list, 
                            agg_col: str, agg_func: str) -> pd.DataFrame:
        # nunique seems to not work in groupby.agg
        # even though https://github.com/dask/dask/pull/8479 seems to resolve it?
        # alternative is to use nunique as series
        if agg_func=="nunique":
            agg_df = (df.groupby(group_cols)[agg_col].nunique()
                    .reset_index()
                 )
        else:
            agg_df = (df.groupby(group_cols)
                      .agg({agg_col: agg_func})
                      .reset_index()
                     )
        
        # return pd.DataFrame for now, since it's not clear what the metadata should be
        # if we are inputting different things in stats_col
        return agg_df.compute()
    
    final = pd.DataFrame()
    
    for agg_col, agg_func in stat_cols.items():
        agg_df = group_and_aggregate(df, group_cols, agg_col, agg_func)
        
        # If it's empty, just add our new table of aggregations in with concat
        if final.empty:
            final = pd.concat([final, agg_df], axis=0, ignore_index=True)
        
        # If it's not empty, do a merge
        else:
            final = pd.merge(
                final, agg_df, on = group_cols, how = "left"
            )
    
    return final


def reshape_long_to_wide(
    df: pd.DataFrame, 
    group_cols: list,
    long_col: str = 'time_of_day',
    value_col: str = 'trips',
    long_col_sort_order: list = ['owl', 'early_am', 'am_peak', 
                                 'midday', 'pm_peak', 'evening'],
    )-> pd.DataFrame:
    """
    To reshape from long to wide, use df.pivot.
    Args in this function correspond this way:
    
    df.pivot(index=group_cols, columns = long_col, values = value_col)
    """
    # To reshape, cannot contain duplicate entries
    # Get it down to non-duplicate form
    # For stop-level, if you're reshaping on value_col==trip, that stop contains
    # the same trip info multiple times.
    df2 = df[group_cols + [long_col, value_col]].drop_duplicates()
    
    #https://stackoverflow.com/questions/22798934/pandas-long-to-wide-reshape-by-two-variables
    reshaped = df2.pivot(
        index=group_cols, columns=long_col,
        values=value_col
    ).reset_index().pipe(to_snakecase)

    # set the order instead of list comprehension, which will just do alphabetical
    add_prefix_cols = long_col_sort_order

    # Change the column order
    reshaped = reshaped.reindex(columns=group_cols + add_prefix_cols)

    # If there are NaNs, fill it with 0, then coerce to int
    reshaped[add_prefix_cols] = reshaped[add_prefix_cols].fillna(0).astype(int)

    # Now, instead columns named am_peak, pm_peak, add a prefix 
    # to distinguish between num_trips and num_stop_arrivals
    reshaped.columns = [f"{value_col}_{c}" if c in add_prefix_cols else c
                            for c in reshaped.columns]

    return reshaped