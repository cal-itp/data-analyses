"""
Cut stop-to-stop segments by shape_id.
 
Start with np.arrays that distance break points for that segment.
Use interpolate to convert distances into shapely points.
Combine shapely points into shapely linestring.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from loguru import logger

from calitp_data_analysis import utils
from segment_speed_utils import array_utils, helpers, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS, CONFIG_PATH)


def get_prior_stop_info(
    gdf: gpd.GeoDataFrame,
    stop_col: str 
) -> gpd.GeoDataFrame:
    """
    For each shape, sort by stop sequence, and fill in the prior stop's 
    projected shape_meters.
    If it's missing, fill it in with itself (no line can be drawn, because it's
    just the same point, but do this so our arrays work downstream). This will
    be the case for the first stop, since there's no segment that 
    can be drawn from the previous stop.
    """
    # Merge in prior stop sequence's stop column info
    prior_stop = gdf[
        ["shape_array_key", "stop_sequence", stop_col]
    ].rename(columns = {
        "stop_sequence": "prior_stop_sequence",
        stop_col: f"prior_{stop_col}"
    }).astype({"prior_stop_sequence": "Int64"})
    
    gdf_with_prior = pd.merge(
        gdf,
        prior_stop,
        on = ["shape_array_key", "prior_stop_sequence"],
        how = "left"
    )
    
    # If we don't have a prior point, fill it in with the same info
    gdf_with_prior[f"prior_{stop_col}"] = (gdf_with_prior[f"prior_{stop_col}"]
                                           .fillna(gdf_with_prior[stop_col])
                                          )

    return gdf_with_prior


def subset_shape_coords_by_indices(
    shape_coords_list: list, 
    indices_array: np.ndarray
) -> list:
    """
    For first stop, there is no prior stop. Return empty list.
    For other stops, return the subset of shape_coords that spans
    the prior to current stop.
    """
    
    if len(indices_array) > 0:
        return array_utils.subset_array_by_indices(
            shape_coords_list, 
            (indices_array[0], indices_array[-1])
            )
    else:
        return np.array([])
    

def linestring_from_points(coords_list: list) -> shapely.geometry.LineString:
    """
    Returns a shapely linestring object. Where there is just 1 point,
    return an empty shapely linestring.
    """
    if len(coords_list) > 1:
        return shapely.geometry.LineString(coords_list)
    else: 
        return shapely.geometry.LineString()


def normal_project(
    shape_geometry: shapely.geometry.LineString,
    shape_projected_distance_array: np.ndarray,
    stop_distance_array: np.ndarray
) -> gpd.GeoDataFrame:
    """
    Use shapely.project. For each stop along the shape, 
    find the subset of relevant coords to keep for our stop_segment.
    Use projected distances as a way to subset the arrays,
    and interpolate it back to shapely points, and string together 
    as shapely linestrings at the end.
    """
    
    # (1) Get list of coords from shapely linestring and convert it to a list
    # of projected distances. 
    # Ex: we can take a subset of distances between stop 2 and 3 [100m, 500m]
    #shape_projected_dist = wrangle_shapes.project_list_of_coords(
    #    shape_geometry, use_shapely_coords = True)
    shape_projected_dist = shape_projected_distance_array
    
    #  (2) Grab a subset of the array
    # for a given pair of (prior_stop, current_stop)
    # This gives us the indices of the subset_array we want
    origin_stop = stop_distance_array[0]
    destination_stop = stop_distance_array[-1]
    
    idx_shape_dist = array_utils.cut_shape_by_origin_destination(
        shape_projected_dist, 
        (origin_stop, destination_stop)
    ) 
    
    # (3) Concatenate the endpoints, so that the segments
    # can get as close as possible to the stop
    subset_shape_dist_with_endpoints = np.concatenate([
        [origin_stop],
        shape_projected_dist[idx_shape_dist],
        [destination_stop]
    ])

    # (4) Interpolate this entire array and convert it from projected distances
    # to shapely points
    subset_shape_geom_with_endpoints = wrangle_shapes.interpolate_projected_points(
        shape_geometry, 
        subset_shape_dist_with_endpoints
    )
    
    # (5) Convert this array into a shapely linestring
    subset_shape_geom_ls = linestring_from_points(subset_shape_geom_with_endpoints)
    
    return subset_shape_geom_ls
    
    
def find_normal_cases_and_setup_df(
    analysis_date: str
) -> gpd.GeoDataFrame:
    
    gdf = pd.read_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}.parquet", 
        filters = [[("loop_or_inlining", "==", 0)]],
        columns = [
            "schedule_gtfs_dataset_key",
            "shape_array_key", "stop_id", "stop_sequence", 
            "shape_meters", 
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
    
    gdf_with_prior = get_prior_stop_info(gdf, "shape_meters")
    
    subset_shapes = gdf.shape_array_key.unique().tolist()
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        filters = [[("shape_array_key", "in", subset_shapes)]],
        crs = PROJECT_CRS,
        get_pandas = True
    ).pipe(helpers.remove_shapes_outside_ca).dropna(subset="geometry")
    
    shapes = shapes.assign(
        shape_projected_dist = shapes.apply(
            lambda x: wrangle_shapes.project_list_of_coords(
                x.geometry, use_shapely_coords = True), 
            axis=1)
    )
    
    gdf_with_shape = pd.merge(
        gdf_with_prior,
        shapes,
        on = "shape_array_key",
        how = "inner"
    )
    
    gdf_with_shape = gdf_with_shape.assign(
        stop_distance_array = gdf_with_shape.apply(
            lambda x: 
            x[["prior_shape_meters", "shape_meters"]].tolist(), axis=1
        ),
    )
    
    return gdf_with_shape
    
    
if __name__ == "__main__":

    LOG_FILE = "../logs/cut_stop_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    EXPORT_FILE = STOP_SEG_DICT["segments_file"]
    
    gdf = find_normal_cases_and_setup_df(analysis_date)
        
    stop_segment_geoseries = []
    
    for row in gdf.itertuples():
        stop_seg_geom = normal_project(
            getattr(row, "geometry"),
            getattr(row, "shape_projected_dist"),
            getattr(row, "stop_distance_array"),
        )
        stop_segment_geoseries.append(stop_seg_geom)
    
    
    gdf = gdf.assign(
        stop_segment_geometry = stop_segment_geoseries
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"Cut normal stop segments: {time1-start}")
    
    keep_cols = [ 
        "schedule_gtfs_dataset_key",
        "shape_array_key", "stop_segment_geometry", 
        "stop_id", "stop_sequence", "loop_or_inlining",
        "stop_primary_direction"
    ]
    
    results_gdf = (gdf[keep_cols]
                   .set_geometry("stop_segment_geometry") 
                   .set_crs(PROJECT_CRS)
                  )
    
    utils.geoparquet_gcs_export(
        results_gdf,
        f"{SEGMENT_GCS}segments_staging/",
        f"{EXPORT_FILE}_normal_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    logger.info(f"export results: {end - time1}")
    logger.info(f"execution time: {end - start}")