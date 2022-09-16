"""
Functions for combining various tables 
from `gtfs_utils` queries. 
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

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