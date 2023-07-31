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

from dask import delayed, compute
from loguru import logger

from shared_utils import utils
from segment_speed_utils import array_utils, helpers, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS, CONFIG_PATH)


def import_stops_projected(analysis_date: str, **kwargs):
    """
    Import stops_projected df, which includes the stop_id, stop_sequence,
    shape_geometry.
    """
    columns = kwargs["columns"]
    
    # If we want to return some kind of geometry, use geopandas, 
    # otherwise use pandas. Grab just the column kwarg for condition check
    STOPS_FILE = f"{SEGMENT_GCS}stops_projected_{analysis_date}/"
    
    if ((columns is None) or 
        (any("geometry" in c for c in columns))):
        df = gpd.read_parquet(STOPS_FILE, **kwargs).drop_duplicates()
        
    else:
        df = pd.read_parquet(STOPS_FILE, **kwargs)

    return df.drop_duplicates().reset_index(drop=True)


def get_prior_shape_meters(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    For each shape, sort by stop sequence, and fill in the prior stop's 
    projected shape_meters.
    If it's missing, fill it in with itself (no line can be drawn, because it's
    just the same point, but do this so our arrays work downstream). This will
    be the case for the first stop, since there's no segment that 
    can be drawn from the previous stop.
    """
    
    shape_cols = ["shape_array_key"]

    gdf = gdf.assign(
        prior_shape_meters = (gdf.sort_values(shape_cols + ["stop_sequence"])
                              .groupby(shape_cols, 
                                       observed=True, group_keys = False)
                              .shape_meters
                              .apply(lambda x: x.shift(1))
                             )
    )
    
    # If it's missing, then set these equal so we don't get Nones when
    # working with shapely points
    gdf = gdf.assign(
        prior_shape_meters = gdf.prior_shape_meters.fillna(gdf.shape_meters)
    )
    
    return gdf


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


def project_and_cut_segments_for_one_shape(
    full_gdf: gpd.GeoDataFrame, 
    one_shape: str
) -> gpd.GeoDataFrame:
    """
    Use shapely.project. For each stop along the shape, 
    find the subset of relevant coords to keep for our stop_segment.
    Use projected distances as a way to subset the arrays,
    and interpolate it back to shapely points, and string together 
    as shapely linestrings at the end.
    """
    gdf = full_gdf[full_gdf.shape_array_key==one_shape].reset_index(drop=True)
    shape_geometry = gdf.geometry.iloc[0]
    
    # (1) Get list of coords from shapely linestring and convert it to a list
    # of projected distances. 
    # Ex: we can take a subset of distances between stop 2 and 3 [100m, 500m]
    shape_projected_dist = wrangle_shapes.project_list_of_coords(
        shape_geometry, use_shapely_coords = True)
    
    #  (2) Grab a subset of the array
    # for a given pair of (prior_stop, current_stop)
    # This gives us the indices of the subset_array we want
    idx_shape_dist = [
        array_utils.cut_shape_by_origin_destination(
            shape_projected_dist, 
            (origin_stop, destination_stop)
        ) for origin_stop, destination_stop in 
        zip(gdf.prior_shape_meters, gdf.shape_meters)
    ]
    
    # (3) Concatenate the endpoints, so that the segments
    # can get as close as possible to the stop
    subset_shape_dist_with_endpoints = [
        np.concatenate([
            [origin_stop],
            shape_projected_dist[idx],
            [destination_stop]
        ]) for idx, origin_stop, destination_stop 
        in zip(idx_shape_dist, gdf.prior_shape_meters, gdf.shape_meters)
    ]

    # (4) Interpolate this entire array and convert it from projected distances
    # to shapely points
    subset_shape_geom_with_endpoints = [ 
        wrangle_shapes.interpolate_projected_points(
            shape_geometry, 
            dist_array
        ) for dist_array in subset_shape_dist_with_endpoints
    ]
    
    # (5) Convert this array into a shapely linestring
    subset_shape_geom_ls = [
        linestring_from_points(i)
        for i in subset_shape_geom_with_endpoints
    ]

    # (6) Assign this geoseries as the stop_segment_geometry column
    # Set the CRS (we lose this)
    # Note: if we change step 5 to a geoseries (with CRS), 
    # we lose the shapely objects, and it's a geoseries of None geometries. 
    # Leave as list, then set CRS here
    
    keep_cols = [
        "schedule_gtfs_dataset_key", "shape_array_key", "stop_segment_geometry", 
        "stop_id", "stop_sequence", "loop_or_inlining"
    ]
    
    gdf2 = (gdf.assign(
        stop_segment_geometry = subset_shape_geom_ls
        )[keep_cols]
        .set_geometry("stop_segment_geometry")
        .set_crs(gdf.crs)
    )
    
    return gdf2
    
    
if __name__ == "__main__":
    import warnings
    
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

    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    EXPORT_FILE = STOP_SEG_DICT["segments_file"]
    
    # Get list of shapes that go through normal stop segment cutting
    shapes_to_cut = import_stops_projected(
        analysis_date, 
        filters = [[("loop_or_inlining", "==", 0)]],
        columns = ["shape_array_key"]
    ).shape_array_key.unique()
    
    gdf = delayed(import_stops_projected)(
        analysis_date,
        filters = [[("loop_or_inlining", "==", 0)]],
        columns = [
            "schedule_gtfs_dataset_key",
            "shape_array_key", "stop_id", "stop_sequence", 
            "shape_meters", 
            "loop_or_inlining",
            "geometry", 
            ]
    )
    
    gdf = (gdf.sort_values(["schedule_gtfs_dataset_key", 
                            "shape_array_key", "stop_sequence"])
           .drop_duplicates(subset=["shape_array_key", "stop_sequence"])
           .dropna(subset="geometry")
           .reset_index(drop=True)
          )

    gdf2 = delayed(get_prior_shape_meters)(gdf).persist()
    
    results = []
    
    for shape in shapes_to_cut:
        segments = delayed(project_and_cut_segments_for_one_shape)(
            gdf2, shape)
        results.append(segments)
    
    time1 = datetime.datetime.now()
    logger.info(f"Cut normal stop segments: {time1-start}")
    
    results2 = [compute(i)[0] for i in results]
    results_gdf = pd.concat(results2, axis=0)
        
    utils.geoparquet_gcs_export(
        results_gdf,
        f"{SEGMENT_GCS}segments_staging/",
        f"{EXPORT_FILE}_normal_{analysis_date}"
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"Export results: {time2-time1}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")