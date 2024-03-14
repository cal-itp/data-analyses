"""
Draw bus corridors (routes -> segments) across all operators.

Takes ~3 min to run 
- down from 6 min in v3
- down from 1 hr in v2 
- down from several hours v1
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys
import zlib

from loguru import logger

from calitp_data_analysis import geography_utils, utils
from shared_utils import rt_utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from update_vars import GCS_FILE_PATH, analysis_date, HQTA_SEGMENT_LENGTH
                        

def difference_overlay_by_route(
    two_directions_gdf: gpd.GeoDataFrame, 
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
    gdf = two_directions_gdf.assign(
        obs = two_directions_gdf.groupby("route_key").cumcount() + 1
    )
    
    keep_cols = ["route_key", "geometry"]

    first = gdf[gdf.obs == 1][keep_cols]
    second = gdf[gdf.obs==2][keep_cols]
    
    route_geom = pd.merge(
        first,
        second,
        on = "route_key",
        how = "inner",
    )
    
    # Find the difference
    # We'll combine it with the first segment anyway
    route_geom = route_geom.assign(
        geometry = route_geom.geometry_x.difference(route_geom.geometry_y)
    ).set_geometry("geometry")    
    
    # Notice that overlay keeps a lot of short segments that are in the
    # middle of the route. Drop these. We mostly want
    # layover spots and where 1-way direction is.
    exploded = (route_geom
                [["route_key", "geometry"]]
                .dissolve(by="route_key")
                .explode(index_parts=True)
                .reset_index()
                .drop(columns = ["level_1"])
               )

    # 750 m is pretty close to how long our hqta segments are,
    # which are 1,250 m. Maybe these segments are long enough to be included.
    CUTOFF = segment_length * 0.5
    
    exploded = exploded.assign(
        overlay_length = exploded.geometry.length,
    ).query(f'overlay_length > {CUTOFF}')
        
    # Now, dissolve it, so it becomes 1 row again
    # Without this initial dissolve, hqta segments will have tiny segments towards ends
    segments_to_attach = (exploded[["route_key", "geometry"]]
                          .dissolve(by="route_key")
                          .reset_index()
                         )
    
    longest_shape_portions = (pd.concat(
        [first, segments_to_attach], axis=0).reset_index(drop=True)
        [["route_key", "geometry"]]
    )
    
    return longest_shape_portions


def select_shapes_and_segment(
    analysis_date: str,
    segment_length: int
) -> gpd.GeoDataFrame: 
    """
    For routes where only 1 shape_id was chosen for longest route_length,
    it's ready to cut into segments.
    
    For routes where 2 shape_ids were chosen...1 in each direction, 
    find the difference.
    
    Concatenate these 2 portions and then cut HQTA segments.
    Returns the hqta_segments for all the routes across all operators.
    """ 
    # Only include certain Amtrak routes
    outside_amtrak_shapes = gtfs_schedule_wrangling.amtrak_trips(
        analysis_date, inside_ca = False).shape_array_key.unique()
    
    gdf = gtfs_schedule_wrangling.longest_shape_by_route_direction(
        analysis_date
    ).query(
        'shape_array_key not in @outside_ca_amtrak_shapes'
    ).drop(
        columns = ["schedule_gtfs_dataset_key", 
                   "shape_array_key", "route_length"]
    ).fillna({"direction_id": 0}).astype({"direction_id": "int"})
    
    routes_both_dir = (gdf.route_key
                       .value_counts()
                       .loc[lambda x: x > 1]
                       .index
                      ).tolist()  

    one_direction = gdf[~gdf.route_key.isin(routes_both_dir)]
    two_directions = gdf[gdf.route_key.isin(routes_both_dir)]   
    
    two_direction_results = difference_overlay_by_route(
        two_directions, segment_length)
        
    ready_for_segmenting = pd.concat(
        [one_direction, two_direction_results], 
        axis=0)[["route_key", "geometry"]].dropna(subset="geometry")
    
    # Cut segments 
    ready_for_segmenting["segment_geometry"] = ready_for_segmenting.apply(
        lambda x: 
        geography_utils.create_segments(x.geometry, int(segment_length)), 
        axis=1, 
    )
    
    segmented = geography_utils.explode_segments(
        ready_for_segmenting, 
        group_cols = ["route_key"],
        segment_col = "segment_geometry"
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
    hqta_segments = hqta_segments.reindex(
        columns = route_cols + cols + ["geometry"])
    
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
    hqta_segments_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    For each hqta_segment_id, grab the origin / destination of 
    each segment. For a route, find the route_direction that appears
    the most, and that's the route_direction to be associated with the route.
    
    Since routes, depending on where you pick origin / destination,
    could have shown both north-south and east-west, doing it this way
    will be more accurate.
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
        with_direction.rename(
            columns = {"route_direction": "segment_direction"}), 
        predominant_direction_by_route,
        on = "route_key",
        how = "left",
        validate = "m:1"
    ).drop(columns = drop_cols)
    
    return routes_with_primary_direction
    
    
if __name__=="__main__":   

    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
                
    # Cut into HQTA segments
    hqta_segments = select_shapes_and_segment(
        analysis_date, HQTA_SEGMENT_LENGTH)
    
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
    
    end = datetime.datetime.now()
    logger.info(f"B1_create_hqta_segments execution time: {end - start}")