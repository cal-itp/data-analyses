"""
Use super_project() to cut loopy or inlining routes.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from dask import delayed, compute
from loguru import logger

import cut_normal_stop_segments
from shared_utils import utils
from segment_speed_utils import array_utils, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, analysis_date


def get_shape_components(
    shape_geometry: shapely.geometry.LineString,
) -> tuple:
    """
    For a shape, we want to get the list of shapely.Points and
    a calculated cumulative distance array.
    """
    shape_coords_list = [shapely.Point(i) for 
                         i in shape_geometry.simplify(0).coords]
    
    # calculate the distance between current point and prior
    # need to remove the first point so that we can 
    # compare to the prior
    point_series_no_idx0 = wrangle_shapes.array_to_geoseries(
        shape_coords_list[1:],
        geom_type="point"
    )

    points_series = wrangle_shapes.array_to_geoseries(
        shape_coords_list, 
        geom_type="point"
    )
    
    distance_from_prior = np.array(
        point_series_no_idx0.distance(points_series)
    )
    
    # Based on distance_from_prior, now create a 
    # cumulative distance array, and append 0 to 
    # the beginning. We want length of this array to match the 
    # length of stop_sequence array
    cumulative_distances = np.array(
        [0] + list(np.cumsum(distance_from_prior))
    )
    
    return shape_coords_list, cumulative_distances


def adjust_stop_start_end_for_special_cases(
    shape_geometry: shapely.geometry.LineString,
    subset_stop_geometry: np.ndarray,
    subset_stop_projected: np.ndarray
) -> tuple:
    """
    """
    # (1a) Normal case: given a current stop and a prior stop
    # use this to get sense of direction, whether 
    # distance is increasing or decreasing as we move from prior to current stop
    if len(subset_stop_projected) > 1:
        start_stop = subset_stop_projected[0]
        end_stop = subset_stop_projected[-1]
    
        # Calculate distance between stops
        distance_between_stops = subset_stop_geometry[0].distance(
            subset_stop_geometry[-1])
    
    # (1b) Origin stop case: current stop only, cannot find prior stop
    # but we can set the start point to be the start of the shape
    else:
        start_stop = 0
        end_stop = subset_stop_projected[0]
        distance_between_stops = subset_stop_projected[0]
        
    # (2) We know distance between stops, so let's back out the correct
    # "end_stop". If the end_stop is actually going 
    # back closer to the start of the shape, we'll use subtraction.
    
    # Normal case
    if start_stop < end_stop:
        origin_stop = start_stop
        destin_stop = start_stop + distance_between_stops
            
    # Case where inlining occurs, and now the bus is doubling back    
    elif start_stop > end_stop:
        origin_stop = start_stop
        destin_stop = start_stop - distance_between_stops
        
    
    # Case at origin, where there is no prior stop to look for
    elif start_stop == end_stop:
        origin_stop = 0
        destin_stop = start_stop
    
    # change this to point
    origin_destination_geom = wrangle_shapes.interpolate_projected_points(
        shape_geometry, [origin_stop, destin_stop]
    )
    
    return origin_stop, destin_stop, origin_destination_geom


def super_project(
    current_stop_seq: int,
    shape_geometry: shapely.geometry.LineString,
    stop_geometry_array: np.ndarray,
    stop_sequence_array: np.ndarray,
):
    """
    
    """
    shape_coords_list, cumulative_distances = get_shape_components(
        shape_geometry)

    # (1) Given a stop sequence value, find the stop_sequence values 
    # just flanking it (prior and subsequent).
    # this is important especially because stop_sequence does not have 
    # to be increasing in increments of 1, but it has to be monotonically increasing
    subset_seq = array_utils.include_prior(
        stop_sequence_array, current_stop_seq)
    
    #https://stackoverflow.com/questions/31789187/find-indices-of-large-array-if-it-contains-values-in-smaller-array
    idx_stop_seq = np.where(np.in1d(
        stop_sequence_array, subset_seq))[0]
    
    # (2) Grab relevant subset based on stop sequence values to get stop geometry subset
    # https://stackoverflow.com/questions/5508352/indexing-numpy-array-with-another-numpy-array    
    subset_stop_geom = array_utils.subset_array_by_indices(
        stop_geometry_array,
        (idx_stop_seq[0], idx_stop_seq[-1])
    )
    
    # (3a) Project this vector of start/end stops
    subset_stop_proj = wrangle_shapes.project_list_of_coords(
        shape_geometry, subset_stop_geom)
    
    
    # (4) Handle various cases for first stop or last stop
    # and also direction of origin to destination stop
    # if destination stop actually is closer to the start of the shape,
    # that's ok, we'll use distance and make sure the direction is correct
    (origin_stop, destin_stop, 
     origin_destination_geom) = adjust_stop_start_end_for_special_cases(
        shape_geometry,
        subset_stop_geom, 
        subset_stop_proj
    )
        
    # (5) Find the subset from cumulative distances
    # that is in between our origin stop and destination stop
    idx_shape_dist = array_utils.cut_shape_by_origin_destination(
        cumulative_distances,
        (origin_stop, destin_stop)
    )
    
    # TODO: how often does this occur?
    if len(idx_shape_dist) == 0:
        subset_shape_geom = []
    
    # Last stop case, where we need to grab all the shape coords up to that 
    # stop, but just in case we run out of indices
    # let's truncate by 1         
    elif len(cumulative_distances) == idx_shape_dist[-1] + 1:
        subset_shape_geom = array_utils.subset_array_by_indices(
            shape_coords_list, 
            (idx_shape_dist[0], idx_shape_dist[-2])
        )
    
    # Normal case, let's grab that last point. 
    # To do so, we need to set the index range to be 1 above that.
    else:
        #len(cumulative_distances) > idx_shape_dist[-1]:
        subset_shape_geom = array_utils.subset_array_by_indices(
            shape_coords_list, 
            (idx_shape_dist[0], idx_shape_dist[-1])
        )
        
    
    # Attach the origin and destination, otherwise the segment
    # will not reach the actual stops, but will just grab the trunk portion
    
    # Normal case
    if origin_stop <= destin_stop:
        subset_shape_geom_with_od = np.array(
            [origin_destination_geom[0]] + 
            subset_shape_geom + 
            [origin_destination_geom[-1]]
        )
    
    
    # Special case, where bus is doubling back, we need to flip the 
    # origin and destination points, but the inside should be in increasing order
    else: 
        # elif origin_stop > destin_stop:        
        subset_shape_geom_with_od = np.flip(
            np.array(
                [origin_destination_geom[-1]] + 
                subset_shape_geom + 
                [origin_destination_geom[0]]
            )
        )
    
    return subset_shape_geom_with_od, origin_destination_geom


def super_project_and_cut_segments_for_one_shape(
    gdf: gpd.GeoDataFrame, 
    one_shape: str
) -> gpd.GeoDataFrame:
    
    gdf2 = gdf[gdf.shape_array_key==one_shape].reset_index(drop=True)

    shape_geometry = gdf2.geometry.iloc[0]
    stop_geometry_array = np.array(gdf2.stop_geometry)
    stop_sequence_array = np.array(gdf2.stop_sequence)
        
    segment_results = [
        # since super_project returns a tuple, just grab 1st item in tuple
        super_project(
            stop_seq, 
            shape_geometry, 
            stop_geometry_array, 
            stop_sequence_array
        )[0] for stop_seq in stop_sequence_array
    ]
    
    segment_ls = [cut_normal_stop_segments.linestring_from_points(i) 
                  for i in segment_results]
    
    keep_cols = [
        "shape_array_key", "stop_segment_geometry", 
        "stop_id", "stop_sequence"
    ]
    
    gdf2 = (gdf2.assign(
        stop_segment_geometry = segment_ls
        )[keep_cols]
        .set_geometry("stop_segment_geometry")
        .set_crs(gdf.crs)
    )
    
    return gdf2 


if __name__ == "__main__":
    import warnings
    
    from dask.distributed import Client
    #https://docs.dask.org/en/latest/futures.html#distributed.Client.upload_file
    # Run this in rt_segment_speeds/: python setup.py bdist_egg
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    #client.upload_file('../dist/segment_speed_utils-0.1.0-py3.9.egg')  
    #client.upload_file('../../dist/shared_utils-1.2.0-py3.9.egg')
    
    warnings.filterwarnings(
        "ignore",
        category=shapely.errors.ShapelyDeprecationWarning) 

    LOG_FILE = "../logs/cut_stop_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    #STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    #EXPORT_FILE = STOP_SEG_DICT["segments_file"]
    
    # Get list of shapes that go through normal stop segment cutting
    shapes_to_cut = pd.read_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}/",
        filters = [[("loop_or_inlining", "==", 1)]],
        columns = ["shape_array_key"]
    ).drop_duplicates().shape_array_key
    
    gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}/",
        filters = [[("loop_or_inlining", "==", 1)]],
        columns = ["shape_array_key",
            "stop_id", "stop_sequence", 
            "stop_geometry", # don't need shape_meters, we need stop_geometry
            "geometry", 
            ]
    )
        
    results = []
    
    for shape in shapes_to_cut:
        segments = delayed(super_project_and_cut_segments_for_one_shape)(
            gdf, shape)
        results.append(segments)
    
    time1 = datetime.datetime.now()
    logger.info(f"Cut special stop segments: {time1-start}")
    
    results2 = [compute(i)[0] for i in results]
    results_gdf = (pd.concat(results2, axis=0)
                   .sort_values(["shape_array_key", "stop_sequence"])
                   .reset_index(drop=True)
                  )
    
    utils.geoparquet_gcs_export(
        results_gdf,
        SEGMENT_GCS,
        f"stop_segments_special_{analysis_date}"
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"Export results: {time2-time1}")
    #client.close()