"""
Draw bus corridors (routes -> segments) across all operators.

Use difference instead of symmetric difference, and we'll
end up with similar results, since we cut segments
across both direction == 0 and direction == 1 now.

Cannot use symmetric difference unless we downgrade pandas to 1.1.3
https://gis.stackexchange.com/questions/414317/gpd-overlay-throws-intcastingnanerror.
Too complicated to change between pandas versions.

Takes 8 min to run 
- down from 1 hr in v2 
- down from several hours v1

TODO: speed up geography_utils.cut_segments to be faster.
7.5 min is spent on this step.
"""
import os
os.environ['USE_PYGEOS'] = '0'
import dask.dataframe as dd
import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import pandas as pd
import sys
import zlib

from loguru import logger
from dask import delayed, compute

import operators_for_hqta
from calitp_data_analysis import geography_utils, utils
from shared_utils import rt_utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from utilities import GCS_FILE_PATH
from update_vars import analysis_date, COMPILED_CACHED_VIEWS
                        
HQTA_SEGMENT_LENGTH = 1_250 # meters


def pare_down_trips_by_route_direction(
    trips_with_geom: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:   
    """
    Given a trips table that has shape geometry attached, 
    keep the longest shape by route_length in each direction.
    
    For LA Metro, out of ~700 unique shape_ids,
    this pares it down to ~115 route_ids.
    Use this pared down shape_ids to get hqta_segments.
    """
    route_dir_cols = ["feed_key", "route_key", "route_id", "direction_id"]
    
    trips_with_geom = (
        trips_with_geom
        .assign(
            route_length = trips_with_geom.geometry.length
       ).sort_values(route_dir_cols + ["route_length"], 
                     ascending = [True for i in route_dir_cols] + [False])
        .drop_duplicates(subset = route_dir_cols)
        .reset_index(drop=True)
    )
        
    # If direction_id is missing, then later code will break, because
    # we need to find the longest route_length
    # Don't really care what direction is, since we will replace it with north-south
    # Just need a value to stand-in, treat it as the same direction
    
    trips_with_geom2 = trips_with_geom.assign(
        direction_id = (trips_with_geom.direction_id.fillna(0)
                        .astype(int).astype(str))
    )

    trips_with_geom2 = trips_with_geom2.assign(    
        route_dir_identifier = trips_with_geom2.apply(
            lambda x: zlib.crc32(
                (x.route_key + x.direction_id
                ).encode("utf-8")), 
            axis=1, 
        )
    )
    
    # Keep the longest shape_id for each direction
    # with missing direction_id filled in
    longest_shapes = (trips_with_geom2.sort_values("shape_array_key")
                      .drop_duplicates("route_dir_identifier")
                      .drop(columns = "route_dir_identifier")
                      .reset_index(drop=True)
                     )

    # A route is uniquely identified by route_key (feed_key + route_id)
    # Once we keep just 1 shape for each route direction, go back to route_key
        
    return longest_shapes


def difference_overlay_by_route(
    longest_shapes: gpd.GeoDataFrame, 
    route: str, 
    segment_length: int
) -> gpd.GeoDataFrame:
    """
    For each route that has 2 directions, do an overlay and 
    find the difference. 
    
    The longest shape is kept. 
    The second shape, which has the difference, should be 
    exploded and expanded to make sure the lengths are long enough.
    If it is, dissolve it.
    
    Keep these portions for a route, and then cut it into segments. 
    """
    # Start with the longest direction (doesn't matter if it's 0 or 1)
    one_route = (longest_shapes[longest_shapes.route_key == route]
                 .sort_values("route_length", ascending=False)
                 .reset_index(drop=True)
            )
    
    first = one_route[one_route.index==0].reset_index(drop=True)
    second = one_route[one_route.index==1].reset_index(drop=True)
    
    # Find the difference
    # We'll combine it with the first segment anyway
    overlay = first.geometry.difference(second.geometry).to_frame(name="geometry")
    
    # Notice that overlay keeps a lot of short segments that are in the
    # middle of the route. Drop these. We mostly want
    # layover spots and where 1-way direction is.
    exploded = (overlay[["geometry"]].dissolve()
                .explode(index_parts=True)
                .reset_index()
                .drop(columns = ["level_0", "level_1"])
               )
    
    exploded2 = exploded.assign(
        overlay_length = exploded.geometry.length,
        route_key = route,
    )
    
    CUTOFF = segment_length * 0.5
    # 750 m is pretty close to how long our hqta segments are,
    # which are 1,250 m. Maybe these segments are long enough to be included.
    
    exploded_long_enough = exploded2[exploded2.overlay_length > CUTOFF]   
    
    # Now, dissolve it, so it becomes 1 row again
    # Without this initial dissolve, hqta segments will have tiny segments towards ends
    segments_to_attach = (exploded_long_enough[["route_key", "geometry"]]
                          .dissolve(by="route_key")
                          .reset_index()
                         )
    
    longest_shape_portions = (pd.concat(
        [first, segments_to_attach], axis=0).reset_index(drop=True)
        [["route_key", "geometry"]]
    )
    
    return longest_shape_portions


def select_shapes_and_segment(
    gdf: gpd.GeoDataFrame,
    segment_length: int) -> gpd.GeoDataFrame: 
    """
    For routes where only 1 shape_id was chosen for longest route_length,
    it's ready to cut into segments.
    
    For routes where 2 shape_ids were chosen...1 in each direction, 
    find the difference.
    
    Concatenate these 2 portions and then cut HQTA segments.
    Returns the hqta_segments for all the routes across all operators.
    
    gpd.overlay(how = 'symmetric_difference') is causing error, 
    either need to downgrade pandas or switch to 'difference'
    https://gis.stackexchange.com/questions/414317/gpd-overlay-throws-intcastingnanerror
    """            
    routes_both_dir = (gdf.route_key
                       .value_counts()
                       .loc[lambda x: x > 1]
                       .index).tolist()  

    one_direction = gdf[~gdf.route_key.isin(routes_both_dir)]
    two_directions = gdf[gdf.route_key.isin(routes_both_dir)]   
    
    two_directions_overlay = gpd.GeoDataFrame()
    
    two_directions_overlay_results = [
        delayed(difference_overlay_by_route)(
            two_directions, r, segment_length)
        for r in routes_both_dir
    ]
  
    two_directions_overlay_results = [
        compute(i)[0] for i in two_directions_overlay_results]
    
    two_direction_results = pd.concat(two_directions_overlay_results, axis=0)
    
    ready_for_segmenting = pd.concat(
        [one_direction, two_direction_results], 
        axis=0)[["route_key", "geometry"]]
    
    # Cut segments 
    segmented = geography_utils.cut_segments(
        ready_for_segmenting,
        group_cols = ["route_key"],
        segment_distance = segment_length
    )
    
    segmented = segmented.assign(
        segment_sequence = (segmented.groupby("route_key")
                            ["segment_sequence"].cumcount())
    )
    
    route_cols = ["feed_key", "route_id", "route_key"]

    # Attach other route info
    hqta_segments = pd.merge(
        segmented,
        gdf[route_cols].drop_duplicates(subset="route_key"),
        on = "route_key",
        how = "inner",
        validate = "m:1"
    )
    
    # Reindex and change column order, put geometry at the end
    cols = [c for c in hqta_segments.columns 
            if c not in route_cols and c != "geometry"]
    hqta_segments = hqta_segments.reindex(columns = route_cols + cols + 
                                          ["geometry"])
    
    # compute (hopefully unique) hash of segment id that can be used
    # across routes/operators
    # this checksum hash always give same value if the same combo of strings are given
    hqta_segments = hqta_segments.assign(
        hqta_segment_id = hqta_segments.apply(
            lambda x: zlib.crc32(
                (x.route_key + 
                 str(x.segment_sequence)).encode("utf-8")), axis=1),
    )
    
    return hqta_segments


def find_primary_direction_across_hqta_segments(
    hqta_segments_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    For each hqta_segment_id, grab the origin / destination of 
    each segment. For a route, find the route_direction that appears
    the most, and that's the route_direction to be associated with the route.
    
    Since routes, depending on where you pick origin / destination,
    could have shown both north-south and east-west, doing it this way
    will be more accurate.
    
    dask can't do the shapely Point(x.coords) operation, 
    so do it on here on segments, which is linestrings.
    Grab the start / endpoint of a linestring
    https://gis.stackexchange.com/questions/358584/how-to-extract-long-and-lat-of-start-and-end-points-to-seperate-columns-from-t
    """
    
    with_od = rt_utils.add_origin_destination(hqta_segments_gdf)
    with_direction = rt_utils.add_route_cardinal_direction(with_od)
    
    # Get predominant direction based on segments
    predominant_direction_by_route = (
        with_direction.groupby(["route_key", "route_direction"])
        .agg({"route_primary_direction": "count"})
        .reset_index()
        .sort_values(["route_key", "route_primary_direction"], 
        # descending order, the one with most counts at the top
        ascending=[True, False])
        .drop_duplicates(subset="route_key")
        .reset_index(drop=True)
        [["route_key", "route_direction"]]
     )
    
    drop_cols = ["origin", "destination", "route_primary_direction"]
    
    routes_with_primary_direction = pd.merge(
        with_direction.drop(columns = "route_direction"), 
        predominant_direction_by_route,
        on = "route_key",
        how = "left",
        validate = "m:1"
    ).drop(columns = drop_cols)
    
    return routes_with_primary_direction


def dissolved_to_longest_shape(hqta_segments: gpd.GeoDataFrame):
    """
    Since HQTA segments were cut right after the overlay difference
    was taken, do a dissolve so that each route is just 1 line geom.
    
    Keep this version to plot the route.
    """
    route_cols = ["feed_key", "route_id", 
                  "route_key", "route_direction"]
    
    dissolved = (hqta_segments[route_cols + ["geometry"]]
                 .dissolve(by=route_cols)
                 .reset_index()
                )
    
    return dissolved
    
    
if __name__=="__main__":   

    logger.add("./logs/B1_create_hqta_segments.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")

    start = dt.datetime.now()
    
    #https://stackoverflow.com/questions/69884348/use-dask-to-chunkwise-work-with-smaller-pandas-df-breaks-memory-limits
        
    # (1) Merge routelines with trips, find the longest shape in 
    # each direction, and after overlay difference, cut HQTA segments
    trips_with_geom = gtfs_schedule_wrangling.get_trips_with_geom(
        analysis_date,
        trip_cols = ["feed_key", "name",
                     "route_key", "route_id", 
                     "direction_id", "shape_array_key"]
    ).dropna(subset="shape_array_key").reset_index(drop=True)

    # Keep longest shape in each direction
    longest_shapes = pare_down_trips_by_route_direction(trips_with_geom)
    
    time1 = dt.datetime.now()
    logger.info(f"merge routes to trips: {time1 - start}")
    
    # Cut into HQTA segments
    hqta_segments = select_shapes_and_segment(
        longest_shapes, HQTA_SEGMENT_LENGTH)
    
    # Since route_direction at the route-level could yield both 
    # north-south and east-west 
    # for a given route, use the segments to determine the primary direction
    hqta_segments_with_dir = find_primary_direction_across_hqta_segments(
        hqta_segments)
    
    utils.geoparquet_gcs_export(
        hqta_segments_with_dir, 
        GCS_FILE_PATH,
        "hqta_segments"
    )
    
    time2 = dt.datetime.now()
    logger.info(f"cut segments: {time2 - time1}")
    
    # In addition to segments, let's keep a version where line geom is 
    # at route-level
    # Dissolve across directions so that each route is 1 row, 1 line
    longest_shape = dissolved_to_longest_shape(hqta_segments_with_dir)
    
    utils.geoparquet_gcs_export(longest_shape,
                                GCS_FILE_PATH,
                                "longest_shape_with_dir"
                               ) 
    
    end = dt.datetime.now()
    logger.info(f"dissolve: {end - time2}")
    logger.info(f"total execution time: {end - start}")