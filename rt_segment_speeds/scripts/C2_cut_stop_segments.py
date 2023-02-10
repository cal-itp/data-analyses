"""
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from dask import delayed
from loguru import logger

from update_vars import SEGMENT_GCS, analysis_date

def get_shape_inputs(row: gpd.GeoDataFrame) -> tuple:
    """
    Since we're using itertuples, use getattr to get that row's column values.
    
    Set up stop_break_dist array with endpoints. 
    We already have shape_meters as an array, just add 0 and the line's length.
    
    Also back out an array for the shape's line geometry to get 
    all the coords for the shape's path.
    """
    stop_break_dist = getattr(row, "shape_meters")
    shape_geom = getattr(row, "shape_geometry")
    
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
    shape_path_dist: list, 
    stop_break_dist: list,
    start_end_tuple: tuple
) -> list:
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
    
    return shape_subset_with_endpoints


def cut_stop_segments_for_shape(row: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    For a row (shape_id), grab the shape_geom, array of 
    stop's shape_meters.
    
    Loop over each stop's shape_meters and grab the subset of relevant
    shape's array values.
    """
    stop_break_dist, shape_path_dist = get_shape_inputs(row)
    
    shape_key = getattr(row, "shape_array_key")
    
    shape_segments = []
    
    for i, _ in enumerate(stop_break_dist):
        # Skip if i == 0, because that's the start of the shape
        # and it has prior element to look against
        
        if i > 0:
            # grab the elements in the array
            # grab the element prior and the current element
            # [, i+1 ] works similar to range(), it just includes i, not i+1
            one_segment = get_shape_coords_up_to_stop(
                shape_path_dist, 
                stop_break_dist,
                stop_break_dist[i-1: i+1]
            )
            
        elif i == 0:
            one_segment = []
        
        shape_segments.append(one_segment)
    
    shape_segment_cutoffs = pd.DataFrame()
    
    shape_segment_cutoffs = shape_segment_cutoffs.assign(
        segment_cutoff_dist = shape_segments,
        shape_meters = pd.Series(stop_break_dist),
        shape_array_key = shape_key,
    )

    return shape_segment_cutoffs
    




def cut_shape_geom_by_stops(
    row: gpd.GeoDataFrame
) -> dg.GeoDataFrame:
    """
    Cut a shape_id's line geometry into segments that correspond to 
    stop-to-stop segments.
    The shape_meters returned from merge_in_shape_geom_and_project()
    gives the distance that stop_id corresponds to somewhere along that line geometry.
    
    We want to cut the line at varying distances that correspond to stops.
    Use interpolate to turn those distances into point geom.
    
    Once it's cut, save the shape_array_key, easier to merge on shapes table.
    """
    break_distances = getattr(row, "shape_meters").tolist()
    line_geom = getattr(row, "shape_geometry")
    shape_key = getattr(row, "shape_array_key")
    stop_sequences = getattr(row, "stop_sequence").tolist()
    
    # https://gis.stackexchange.com/questions/203048/split-lines-at-points-using-shapely/203068
    # First coords of line (start + end)
    coords = [line_geom.coords[0], line_geom.coords[-1]] 
    
    break_points = [line_geom.interpolate(i).coords[0] 
                    for i in break_distances]
    
    coords_with_breaks = [line_geom.coords[0]] + break_points + [line_geom.coords[-1]]
    
    # Add the origin/destination shape_meters (0 and whatever the length is)
    break_distances_with_endpoints = break_distances + [0, line_geom.length]
    
    # Don't use sorted() because it cuts segments weird
    coords_ordered = [
        p for (d, p) in #sorted(
        zip(break_distances_with_endpoints, coords_with_breaks)  #)
    ]
 
    lines = [shapely.geometry.LineString(
                [coords_ordered[i], coords_ordered[i+1]]
            ) for i in range(len(coords_ordered)-1)]
    
    
    # https://shapely.readthedocs.io/en/stable/migration.html#creating-numpy-arrays-of-geometry-objects
    # To avoid shapely deprecation warning, create an empty array
    # and then fill it list's elements (shapely linestrings)
    #arr = np.empty(len(lines), dtype="object")
    #arr[:] = lines
    # the warnings still come up even when we use arrays
    
    lines_geo = gpd.GeoDataFrame(
        lines,
        columns = ["geometry"],
        crs = "EPSG:3310"
    )
    
    lines_geo = lines_geo.assign(
        shape_array_key = shape_key,
        segment_sequence = lines_geo.index,
        stop_sequence = pd.Series(stop_sequences)
    ).set_geometry("geometry")
  
    gddf = dg.from_geopandas(lines_geo, npartitions=1)
    
    return gddf




if __name__ == "__main__":
    
    start = datetime.datetime.now()

    gdf = gpd.read_parquet("./data/stops_projected_wide.parquet")
        
    segment_cutoffs = pd.DataFrame()

    for row in df.itertuples():
        shape_segment_cutoffs = cut_stop_segments_for_shape(row)
    
    segment_cutoffs = pd.concat(
        [segment_cutoffs, shape_segment_cutoffs], 
        axis=0, ignore_index=True)
    
    
    for shape_row in gdf.itertuples():
        start_row = datetime.datetime.now()
        shape_key = getattr(shape_row, "shape_array_key")
        
        shape_stop_segments = delayed(cut_shape_geom_by_stops)(shape_row)

        results.append(shape_stop_segments)
        
        end_row = datetime.datetime.now()
        logger.info(f"finished {shape_key}:  {end_row-start_row}")
                        
    time3 = datetime.datetime.now()
    logger.info(f"cut stop-to-stop segments for shapes: {time3-time2}")
    
    dask_utils.compute_and_export(
        results, 
        gcs_folder = SEGMENT_GCS, 
        file_name = f"stop_segments_{analysis_date}", 
        export_single_parquet = False
    )
    
    end = datetime.datetime.now()
