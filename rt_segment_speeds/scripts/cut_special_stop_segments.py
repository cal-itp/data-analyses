"""
Use super_project() to cut loopy or inlining routes.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from loguru import logger

import cut_normal_stop_segments
from calitp_data_analysis import utils
from segment_speed_utils import (array_utils, helpers, 
                                wrangle_shapes)
from segment_speed_utils.project_vars import (SEGMENT_GCS,
                                              CONFIG_PATH, PROJECT_CRS)


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
    
    distance_from_prior = np.asarray(
        point_series_no_idx0.distance(points_series)
    )
    
    # Based on distance_from_prior, now create a 
    # cumulative distance array, and append 0 to 
    # the beginning. We want length of this array to match the 
    # length of stop_sequence array
    cumulative_distances = np.asarray(
        [0] + list(np.cumsum(distance_from_prior))
    )
    
    return shape_coords_list, cumulative_distances


def adjust_stop_start_end_for_special_cases(
    shape_geometry: shapely.geometry.LineString,
    subset_stop_geometry: np.ndarray,
    subset_stop_projected: np.ndarray
) -> tuple:
    """
    Given two stops, look at what how distance is increasing or 
    decreasing.
    Calculate the distance between them.
    Pin ourselves to the prior stop, use distance between, to get 
    at where we are going for current stop.
    With our new calculated start/end projected points, interpolate it
    and get back our point geometries.
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
        
        # The above is straight line distance, so we can possibly underestimate
        # Check if there's any leftover distance 
        end_stop_interp = wrangle_shapes.interpolate_projected_points(
            shape_geometry, [end_stop]
        )[0]
        
        leftover_distance = end_stop_interp.distance(subset_stop_geometry[-1])
        
        distance_best_guess = distance_between_stops + leftover_distance
    
    # (1b) Origin stop case: current stop only, cannot find prior stop
    # but we can set the start point to be the start of the shape
    else:
        start_stop = 0
        end_stop = subset_stop_projected[0]
        distance_between_stops = subset_stop_projected[0]
        distance_best_guess = distance_between_stops
        
    # (2) We know distance between stops, so let's back out the correct
    # "end_stop". We use a cumulative distance array...just need distance between 
    # 2 points
    
    # Normal case or inlining
    if start_stop != end_stop:
        origin_stop = start_stop
     
        destin_stop = start_stop + distance_best_guess
        
    # Case at origin, where there is no prior stop to look for
    elif start_stop == end_stop:
        origin_stop = wrangle_shapes.project_list_of_coords(
            shape_geometry, 
            use_shapely_coords = True
        )[0]
        destin_stop = start_stop
    
    # change this to point
    origin_destination_geom = wrangle_shapes.interpolate_projected_points(
        shape_geometry, [origin_stop, destin_stop]
    )
    
    # Check what direction the stops run
    stop_vec = wrangle_shapes.get_direction_vector(
        subset_stop_geometry[0],
        subset_stop_geometry[-1]
    )
    
    norm_stop_vec = wrangle_shapes.get_normalized_vector(stop_vec) 
    
    # Check what direction the segment runs
    segment_vec = wrangle_shapes.get_direction_vector(
        origin_destination_geom[0], 
        origin_destination_geom[-1]
    )
    
    norm_segment_vec = wrangle_shapes.get_normalized_vector(segment_vec)
    
    dot_product = wrangle_shapes.dot_product(norm_stop_vec, norm_segment_vec) 
    
    if dot_product < 0:
        flip = True
    else: 
        flip = False
    
    return origin_stop, destin_stop, origin_destination_geom, flip


def super_project(
    shape_geometry: shapely.geometry.LineString,
    stop_geometry_array: np.ndarray,
) -> shapely.geometry.LineString:
    """
    Implement super project for one stop. 
    """
    shape_coords_list, cumulative_distances = get_shape_components(shape_geometry)
    
    subset_stop_geom = stop_geometry_array
    
    # (1) Project this vector of start/end stops
    subset_stop_proj = wrangle_shapes.project_list_of_coords(
        shape_geometry, subset_stop_geom)
    
    
    # (2) Handle various cases for first stop or last stop
    # and grab the distance between origin/destination stop to use 
    # with cumulative distance array
    (origin_stop, destin_stop, 
     origin_destination_geom, flip) = adjust_stop_start_end_for_special_cases(
        shape_geometry,
        subset_stop_geom, 
        subset_stop_proj
    )
        
    # (3) Find the subset from cumulative distances
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
    subset_shape_geom_with_od = np.array(
        [origin_destination_geom[0]] + 
        subset_shape_geom + 
        [origin_destination_geom[-1]]
    )
    
    # flip happens here for inlining.
    if flip:
        subset_shape_geom_with_od = np.flip(subset_shape_geom_with_od)
    
    subset_ls = cut_normal_stop_segments.linestring_from_points(
        subset_shape_geom_with_od)
    
    return subset_ls 


def find_special_cases_and_setup_df(
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Import just special cases and prep so we can apply super_project row-wise.
    For every stop, we need to attach the array of stop geometry for that shape.
    """
    gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}.parquet", 
        filters = [[("loop_or_inlining", "==", 1)]],
        columns = [
            "schedule_gtfs_dataset_key",
            "shape_array_key", "stop_id", "stop_sequence", 
            "stop_geometry", # don't need shape_meters, we need stop_geometry 
            "loop_or_inlining",
            "st_trip_instance_key",
            "prior_stop_sequence",
            "stop_primary_direction"
        ]
    ).sort_values(
        "st_trip_instance_key"
    ).drop_duplicates(
        subset=["shape_array_key", "stop_sequence"]
    ).reset_index(drop=True)
    
    # Merge in prior stop sequence's stop geom
    gdf_with_prior = cut_normal_stop_segments.get_prior_stop_info(
        gdf, "stop_geometry"
    )

    subset_shapes = gdf_with_prior.shape_array_key.unique().tolist()
    
    # Merge in shape geometry, but first remove shapes that might make 
    # kernel crash...shapes that are really long and run nationwide
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        filters = [[("shape_array_key", "in", subset_shapes)]],
        crs = PROJECT_CRS,
        get_pandas = True
    ).pipe(helpers.remove_shapes_outside_ca)
    
    gdf_with_shape = pd.merge(
        gdf_with_prior,
        shapes,
        on = "shape_array_key",
        how = "inner"
    )
    
    gdf_with_shape = gdf_with_shape.assign(
        stop_geometry_array = gdf_with_shape.apply(
            lambda x: 
            x[["prior_stop_geometry", "stop_geometry"]].tolist(), axis=1
        )
    )
    
    return gdf_with_shape


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date

    LOG_FILE = "../logs/cut_stop_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    EXPORT_FILE = STOP_SEG_DICT["segments_file"]
    
    start = datetime.datetime.now()
    
    gdf = find_special_cases_and_setup_df(analysis_date)
        
    stop_segment_geoseries = []
    
    for row in gdf.itertuples():
        stop_seg_geom = super_project(
            getattr(row, "geometry"),
            getattr(row, "stop_geometry_array"),
        )
        stop_segment_geoseries.append(stop_seg_geom)
    
    
    gdf = gdf.assign(
        stop_segment_geometry = stop_segment_geoseries
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"Cut special stop segments: {time1-start}")
    
    keep_cols = [ 
        "schedule_gtfs_dataset_key",
        "shape_array_key", "stop_segment_geometry", 
        "stop_id", "stop_sequence", "loop_or_inlining",
        "stop_primary_direction"
    ]
    
    results_gdf = (gdf[keep_cols]
                   .set_geometry("stop_segment_geometry") 
                   .set_crs(gdf.crs)
                  )

    utils.geoparquet_gcs_export(
        results_gdf,
        f"{SEGMENT_GCS}segments_staging/",
        f"{EXPORT_FILE}_special_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    logger.info(f"export results: {end - time1}")
    logger.info(f"execution time: {end - start}")
    