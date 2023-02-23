"""
Cut stop-to-stop segments by shape_id.
 
Start with np.arrays that distance break points for that segment.
Use interpolate to convert distances into shapely points.
Combine shapely points into shapely linestring.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from dask import delayed
from loguru import logger

from shared_utils import dask_utils, utils
from segment_speed_utils import sched_rt_utils
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS)

def get_shape_inputs(row: gpd.GeoDataFrame) -> tuple:
    """
    Since we're using itertuples, use getattr to get that row's column values.
    
    Set up stop_break_dist array with endpoints. 
    We already have shape_meters as an array, just add 0 and the line's length.
    
    Also back out an array for the shape's line geometry to get 
    all the coords for the shape's path.
    """
    stop_break_dist = getattr(row, "shape_meters")
    shape_geom = getattr(row, "geometry")
    
    stop_break_dist_with_endpoints = np.array(
        [0] + stop_break_dist.tolist() + [shape_geom.length]
    )
    
    # Get all the distances for all the 
    # coordinate points included in shape line geom
    shape_path_dist = np.array(
        [shape_geom.project(shapely.geometry.Point(p)) 
        for p in shape_geom.coords]
    )
    
    return stop_break_dist_with_endpoints, shape_path_dist


def get_shape_coords_up_to_stop(
    shape_geom: shapely.geometry.LineString,
    shape_path_dist: list, 
    stop_break_dist: list,
    start_end_tuple: tuple
) -> shapely.geometry.LineString:
    """
    For every pair of start/end shape_meters, grab all the 
    shape path's coords in between.
    
    Ex: if (start, end) = (50, 150), then grab the subset of the 
    shape distance array that spans [50, 150], which could be 
    [50, 55, 70, 100, 120, 135, 150]
    """
    start_dist, end_dist = start_end_tuple
    
    # Get the subset of shape_path points that
    # covers start_dist to end_dist
    # https://stackoverflow.com/questions/16343752/numpy-where-function-multiple-conditions
    shape_path_subset = shape_path_dist[
        (np.where(
            (shape_path_dist >= start_dist) & 
            (shape_path_dist <= end_dist))
        )]
    
    # Now add the start_dist and end_dist to the subset
    shape_subset_with_endpoints = np.unique(np.array(
            [start_dist] + shape_path_subset.tolist() + [end_dist]))
    
    # In each row, get the array, and interpolate it to convert 
    # distances to points and string together into line geometry
    segment_points = [shape_geom.interpolate(i) 
                      for i in shape_subset_with_endpoints]

    if len(segment_points) >= 2:
        segment_line = shapely.geometry.LineString(segment_points)
    else:
        segment_line = np.nan
    
    return segment_line


def cut_stop_segments_for_shape(row: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    For a row (shape_id), grab the shape_geom, array of 
    stop's shape_meters.
    
    Loop over each stop's shape_meters and grab the subset of relevant
    shape's array values.
    """
    stop_break_dist, shape_path_dist = get_shape_inputs(row)
    
    shape_key = getattr(row, "shape_array_key")
    shape_geom = getattr(row, "geometry")
    
    shape_segments = []
    
    # Use np.indices instead of enumerate
    for i in np.indices(stop_break_dist.shape).flatten():
        # Skip if i == 0, because that's the start of the shape
        # and it has prior element to look against
        
        # We want to skip the first element and the last element
        # The last point on the shape runs just past the last stop
        if (i > 0) and (i < np.indices(stop_break_dist.shape).flatten().argmax()):
            # grab the elements in the array
            # grab the element prior and the current element
            # [, i+1 ] works similar to range(), it just includes i, not i+1
            one_segment = get_shape_coords_up_to_stop(
                shape_geom,
                shape_path_dist, 
                stop_break_dist,
                stop_break_dist[i-1: i+1]
            )
            
            shape_segments.append(one_segment)
 
    
    shape_segment_cutoffs = pd.DataFrame()
    
    shape_segment_cutoffs = shape_segment_cutoffs.assign(
        geometry = shape_segments,
        shape_meters = pd.Series(stop_break_dist),
        shape_array_key = shape_key,
    )
    
    return shape_segment_cutoffs
   
    
def clean_up_stop_segments(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    After cutting stop-to-stop segments, do some cleanup.
    There will be some None geometries and possibly duplicates.
    Handle them here.
    """
    # Drop None geometries
    gdf = gdf[gdf.geometry.notna()].reset_index(drop=True)

    # Look for duplicates - split df into duplicated and not duplicated
    # https://stackoverflow.com/questions/22904523/select-rows-with-duplicate-observations-in-pandas
    shape_cols = ["shape_array_key", "shape_meters"]
    
    duplicated_df = gdf[gdf.duplicated(
        subset=shape_cols, keep=False) == True]
    
    rest_of_df = gdf[~gdf.duplicated(subset=shape_cols, keep=False)]
        
    # If there are duplicates, we will keep the row with more 
    # coords (more points) to form the line
    duplicated_df = duplicated_df.assign(
        num_coords = duplicated_df.geometry.apply(
            lambda x: len(x.coords))
    )
    
    no_dups = (duplicated_df.sort_values(
        shape_cols + ["num_coords"], ascending=[True, True, False])
        .drop_duplicates(subset=shape_cols)
        .drop(columns = "num_coords")
    )
        
    # Concatenate 
    cleaned_df = (pd.concat([rest_of_df, no_dups], axis=0)
                  .sort_values(shape_cols)
                  .reset_index(drop=True)
                 ) 
    
    return cleaned_df    
    
    
if __name__ == "__main__":
    import warnings
    
    warnings.filterwarnings(
        "ignore",
        category=shapely.errors.ShapelyDeprecationWarning) 

    logger.add("../logs/C2_cut_stop_segments.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    df = gpd.read_parquet(
        f"{SEGMENT_GCS}stops_projected_wide_{analysis_date}.parquet")
    
    segment_cutoffs = gpd.GeoDataFrame()
    
    for row in df.itertuples():
        shape_segment_cutoffs = cut_stop_segments_for_shape(row)
        
        segment_cutoffs = pd.concat(
            [segment_cutoffs, shape_segment_cutoffs], 
            axis=0, ignore_index=True)
        

    time1 = datetime.datetime.now()
    logger.info(f"cut stop-to-stop segments and save projected "
                f"coords: {time1-start}")
    
    segments_assembled = gpd.GeoDataFrame(
        segment_cutoffs[
            segment_cutoffs.shape_meters > 0
        ].reset_index(drop=True),
        geometry = "geometry", 
        crs = PROJECT_CRS)
    
    time2 = datetime.datetime.now()
    logger.info(f"assemble stop-to-stop segments: {time2-time1}")
    
    # Clean up stop-to-stop segments
    cleaned_segments = clean_up_stop_segments(segments_assembled)

    # Merge in the stop projected geom so that each stop can be 
    # attached to the segment leading up to that stop
    stops_projected = gpd.read_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}.parquet")
    
    stops_with_cleaned_segments = pd.merge(
        stops_projected.drop(columns = "shape_geometry").rename(
            columns = {"geometry": "stop_geometry"}),
        cleaned_segments,
        on = ["shape_array_key", "shape_meters"],
        # we want to keep left only, because in generating 
        # stop segments, we intentionally skipped first stop, 
        # so there's definitely going to be left_only observations
        how = "left",
        validate = "m:1",
    )
    
    stop_segments_with_rt_key = sched_rt_utils.add_rt_keys_to_segments(
        stops_with_cleaned_segments, 
        analysis_date, 
        ["feed_key", "shape_array_key"])
    
    utils.geoparquet_gcs_export(
        stop_segments_with_rt_key,
        SEGMENT_GCS,
        f"stop_segments_{analysis_date}"
    )

    time3 = datetime.datetime.now()
    logger.info(f"Clean up stop segments and attach to stop geom and export: {time3-time2}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")