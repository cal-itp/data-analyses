"""
Functions for applying shapely project and interpolation.
Move our shapes (linestrings) and stops (points) from coordinates
to numpy arrays with numeric values (shape_meters) and vice versa.
"""
import dask.array as da
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import numpy as np
import pandas as pd

from segment_speed_utils.project_vars import PROJECT_CRS

def merge_in_segment_shape(
    vp: dd.DataFrame, 
    segments: gpd.GeoDataFrame, 
    segment_identifier_cols: list
) -> dd.DataFrame:
    """
    Do linear referencing and calculate `shape_meters` for the 
    enter/exit points on the segment. 
    
    This allows us to calculate the distance_elapsed.
    
    Return dd.DataFrame because geometry makes the file huge.
    Merge in segment geometry later before plotting.
    """
    
    if isinstance(segments, dg.GeoDataFrame):
        segments = segments.compute()
    
    # https://stackoverflow.com/questions/71685387/faster-methods-to-create-geodataframe-from-a-dask-or-pandas-dataframe
    # https://github.com/geopandas/dask-geopandas/issues/197
    vp = vp.assign(
        geometry = dg.points_from_xy(vp, "lon", "lat", crs = PROJECT_CRS), 
    )
    
    # Refer to the geometry column by name
    vp_gddf = dg.from_dask_dataframe(
        vp, 
        geometry="geometry"
    )
    
    linear_ref_vp_to_shape = dd.merge(
        vp_gddf, 
        segments[segments.geometry.notna()
                ][segment_identifier_cols + ["geometry"]],
        on = segment_identifier_cols,
        how = "inner"
    )
    
    # Convert to geoseries so that we can do the project
    vp_geoseries = gpd.GeoSeries(linear_ref_vp_to_shape.geometry_x.compute())
    shape_geoseries = gpd.GeoSeries(linear_ref_vp_to_shape.geometry_y.compute())
    
    # Project, save results, then convert to dask array, 
    # otherwise can't add a column to the dask df
    shape_meters_geoseries = da.array(shape_geoseries.project(vp_geoseries))

    # https://www.appsloveworld.com/coding/dataframe/6/add-a-dask-array-column-to-a-dask-dataframe
    linear_ref_vp_to_shape['shape_meters'] = shape_meters_geoseries
    
    linear_ref_df = (linear_ref_vp_to_shape.drop(
        columns = ["geometry_x", "geometry_y",
                   "lon", "lat"])
                     .drop_duplicates()
                     .reset_index(drop=True)
    )
    
    return linear_ref_df