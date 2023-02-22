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

from shared_utils import geography_utils, utils, rt_utils
from segment_speed_utils import sched_rt_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, analysis_date


def merge_routes_to_trips(
    routelines: dg.GeoDataFrame, trips: dd.DataFrame
) -> dg.GeoDataFrame:   
    """
    Merge routes and trips tables.
    Keep the longest shape by route_length in each direction.
    
    For LA Metro, out of ~700 unique shape_ids,
    this pares it down to ~115 route_ids.
    Use this pared down shape_ids to get hqta_segments.
    """
    shape_id_cols = ["shape_array_key"]
    route_dir_cols = ["feed_key", "route_key", "route_id", "direction_id"]
    
    # Merge routes to trips with using trip_id
    # Keep route_id and shape_id, but drop trip_id by the end
    # Use pandas instead of dask because we want to sort by multiple columns
    # and then drop_duplicates and longest route_length
    trips_with_geom = dd.merge(
        routelines,
        trips,
        on = shape_id_cols,
        how = "inner",
    ).compute()
    
    
    trips_with_geom = (trips_with_geom
                       .assign(
                            route_length = trips_with_geom.geometry.length
                       ).sort_values(route_dir_cols + ["route_length"], 
                                     ascending=[True, True, True, True, False])
                       .drop_duplicates(subset = route_dir_cols)
                       .reset_index(drop=True)
                      )
    
    m1 = dg.from_geopandas(trips_with_geom, npartitions=2)
    
    # If direction_id is missing, then later code will break, because
    # we need to find the longest route_length
    # Don't really care what direction is, since we will replace it with north-south
    # Just need a value to stand-in, treat it as the same direction
    # in v2, direction_id is float
    m1 = m1.assign(
        direction_id = m1.direction_id.fillna(0),
    )
    
    m1 = m1.assign(    
        route_dir_identifier = m1.apply(
            lambda x: zlib.crc32(
                (x.route_key + str(x.direction_id)
                ).encode("utf-8")), 
            axis=1, meta=('route_dir_identifier', 'int'))
    )
    
    # Keep the longest shape_id for each direction
    # with missing direction_id filled in
    longest_shapes = (m1.sort_values("shape_id")
                      .drop_duplicates("route_dir_identifier")
                      .rename(columns = {"shape_id": "longest_shape_id"})
                     )
        
    return longest_shapes


def get_longest_shapes(analysis_date: str) -> dg.GeoDataFrame:
    trips = sched_rt_utils.get_scheduled_trips(analysis_date)        
    routelines = sched_rt_utils.get_routelines(analysis_date)

    longest_shapes = merge_routes_to_trips(routelines, trips)
        
    return longest_shapes


def add_arrowized_geometry(gdf: dg.GeoDataFrame) -> dg.GeoDataFrame:
    """
    Add a column where the route segment is arrowized.
    """
    if isinstance(gdf, gpd.GeoDataFrame):
        gdf = dg.from_geopandas(gdf, npartitions=3) 
        
    gdf = gdf.assign(
        geometry_arrowized = gdf.apply(
            lambda x: rt_utils.try_parallel(x.geometry), 
            axis=1, 
            meta = ("geometry_arrowized", "geometry")
        )
    )
    
    gdf = gdf.assign(
        geometry_arrowized = gdf.apply(
            lambda x: rt_utils.arrowize_segment(
                x.geometry_arrowized, buffer_distance = 20),
            axis = 1,
            meta = ('geometry_arrowized', 'geometry')
        )
    )

    return gdf


if __name__ == "__main__":
    logger.add("../logs/A2_cut_route_segments.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    longest_shapes = get_longest_shapes(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"Get longest shapes: {time1 - start}")
    
    # Cut segments
    segments = geography_utils.cut_segments(
        longest_shapes,
        group_cols = ["feed_key", "name",
                      "route_id", "direction_id", "longest_shape_id",
                      "route_dir_identifier", "route_length"],
        segment_distance = 1_000
    )
    
    segments = sched_rt_utils.add_rt_keys_to_segments(
        segments, 
        analysis_date,
        ["feed_key", "route_id", "direction_id"]
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"Cut route segments {time2 - time1}")
    
    # Add arrowized geometry
    arrowized_segments = add_arrowized_geometry(segments).compute()
    
    time3 = datetime.datetime.now()
    logger.info(f"Add arrowized geometry: {time3 - time2}")

    utils.geoparquet_gcs_export(
        arrowized_segments,
        SEGMENT_GCS,
        f"longest_shape_segments_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
