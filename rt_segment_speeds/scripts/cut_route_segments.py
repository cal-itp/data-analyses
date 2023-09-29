"""
Create route segments. 

For now, use the code from HQTA and lift it completely.

Create a crosswalk where a trip's `route_id-direction_id` can 
be merged to find the `route_dir_identifier`. 
The `route_dir_identifier` is used for segments to cut segments
for both directions the route runs.

From the trip table, rather than going to shape_id, as long as route
and direction info is present, we have already cut the segments 
and stored what the longest_shape_id is for that route.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys
import zlib

from loguru import logger

from calitp_data_analysis import geography_utils, utils
from segment_speed_utils import (gtfs_schedule_wrangling, helpers, 
                                 sched_rt_utils, wrangle_shapes)
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)


def longest_shape_and_add_route_dir_identifier(
    trips_with_geom: dg.GeoDataFrame
) -> dg.GeoDataFrame:   
    """
    For trips that has shape geom attached, keep the longest 
    shape_id for each route_id-direction_id.
    """
    
    if isinstance(trips_with_geom, dg.GeoDataFrame):
        trips_with_geom = trips_with_geom.compute()
        
    route_dir_cols = ["route_key", "route_id", "direction_id"]
    
    trips_with_geom = (
        trips_with_geom
        .assign(
            route_length = trips_with_geom.geometry.length
        ).sort_values(
           route_dir_cols + ["route_length"], 
           ascending = [True for i in route_dir_cols ] +[False])
       .drop_duplicates(subset = route_dir_cols)
       .reset_index(drop=True)
    )

    gddf = dg.from_geopandas(trips_with_geom, npartitions=1)
    
    
    gddf = gddf.assign(    
        route_dir_identifier = gddf.apply(
            lambda x: zlib.crc32(
                (x.route_key + str(x.direction_id)
                ).encode("utf-8")), 
            axis=1, meta=('route_dir_identifier', 'int'))
    )
    
    # Sort and keep one row for each route-direction
    # It's possible that there are still multiple rows left, because they're
    # all the longest route_length
    longest_shapes = (gddf.sort_values("shape_id")
                      .drop_duplicates("route_dir_identifier")
                      .rename(columns = {"shape_id": "longest_shape_id"})
                     )
    
    return longest_shapes   


def prep_route_segments(analysis_date: str):
    """
    Prep route segments gdf by merging trips with shapes.
    Pare this down to the longest shape_id by each route-direction.
    """
    shape_id_cols = ["shape_array_key"]
    route_dir_cols = ["feed_key", "route_key", 
                      "route_id", "direction_id"]
    
    trips = helpers.import_scheduled_trips(
        analysis_date, 
        columns = route_dir_cols + shape_id_cols + [
            "trip_id", "shape_id", "name"],
        get_pandas = True
    )       
    
    trips = gtfs_schedule_wrangling.exclude_scheduled_operators(
        trips, 
        exclude_me = ["Amtrak Schedule"]
    )
        
    shapes = helpers.import_scheduled_shapes(analysis_date)

    trips_with_geom = gtfs_schedule_wrangling.merge_shapes_to_trips(
        shapes,
        trips
    )
    
    longest_shapes = longest_shape_and_add_route_dir_identifier(trips_with_geom)
    
    return longest_shapes
 
    
def prep_and_cut_route_segments(analysis_date: str): 
    longest_shapes = prep_route_segments(analysis_date)
    
    group_cols = [
        "feed_key", "name",
        "route_id", "direction_id", "longest_shape_id",
        "route_dir_identifier"
    ]
    
    # Cut route segments
    segments = geography_utils.cut_segments(
        longest_shapes,
        group_cols = group_cols,
        segment_distance = 1_000
    )
    
    return segments


def finalize_route_segments(route_segments: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """    
    Arrowize geometry and also add gtfs_dataset_key.
    """
    route_segments_with_rt_key = sched_rt_utils.add_rt_keys_to_segments(
        route_segments, 
        analysis_date, 
        ["feed_key", "route_id", "direction_id"]
    )
    
    # arrowize 
    arrowized_segments = wrangle_shapes.add_arrowized_geometry(
        route_segments_with_rt_key)
    
    return arrowized_segments

    
if __name__ == "__main__":
    LOG_FILE = "../logs/cut_route_segments.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    EXPORT_FILE = ROUTE_SEG_DICT["segments_file"]
    
    # Merge dfs and cut into equally sized segments
    segments = prep_and_cut_route_segments(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"Prep and cut equally sized route segments: {time1 - start}")
    
    arrowized_segments = finalize_route_segments(segments)
    time2 = datetime.datetime.now()
    logger.info(f"Add rt key and arrowized geometry: {time2 - time1}")

    utils.geoparquet_gcs_export(
        arrowized_segments,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
