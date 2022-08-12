"""
Functions for creating bus corridors.

Mostly more processing of routelines, trips, stop times tables
to get it ready for finding high quality transit corridors.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd
import zlib

from shapely.geometry import LineString, Point

import utilities
from shared_utils import rt_utils, gtfs_utils

#------------------------------------------------------#
# Stop times
#------------------------------------------------------#
def stop_times_aggregation_by_hour(stop_times: dd.DataFrame) -> dd.DataFrame:
    """
    Take the stop_times table 
    and group by stop_id-departure hour
    and count how many trips occur.
    """
    stop_cols = ["calitp_itp_id", "stop_id"]

    ddf = gtfs_utils.fix_departure_time(stop_times)
    
    # Aggregate how many trips are made at that stop by departure hour
    trips_per_hour = (ddf.groupby(stop_cols + ["departure_hour"])
                      .agg({'trip_id': 'count'})
                      .reset_index()
                      .rename(columns = {"trip_id": "n_trips"})
                     )    
    
    return trips_per_hour


def find_stop_with_high_trip_count(
    stop_times: dd.DataFrame) -> dd.DataFrame: 
    """
    Take the stop_times table, and
    count how many trips pass through that stop_id
    """
    stop_cols = ["calitp_itp_id", "stop_id"]

    trip_count_by_stop = (stop_times
                          .groupby(stop_cols)
                          .agg({"trip_id": "count"})
                          .reset_index()
                          .rename(columns = {"trip_id": "n_trips"})
                          .sort_values(["calitp_itp_id", "n_trips"], 
                                       ascending=[True, False])
                          .reset_index(drop=True)
                         )
    
    return trip_count_by_stop


#------------------------------------------------------#
# Routelines
#------------------------------------------------------#
def merge_routes_to_trips(routelines: dg.GeoDataFrame, 
                          trips: dd.DataFrame) -> dg.GeoDataFrame:   
    """
    Merge routes and trips tables.
    
    For LA Metro, out of ~700 unique shape_ids,
    this pares it down to ~115 route_ids.
    Use this pared down shape_ids to get hqta_segments.
    """
    routelines_ddf = routelines.assign(
        route_length = routelines.geometry.length,
        # Easier to drop if we make sure centroid of that line segment is the same too
        x = routelines.geometry.centroid.x.round(3),
        y = routelines.geometry.centroid.y.round(3),
    )
        
    # Merge routes to trips with using trip_id
    # Keep route_id and shape_id, but drop trip_id by the end
    shape_id_cols = ["calitp_itp_id", "shape_id"]
    
    m1 = (dd.merge(
            routelines_ddf,
            # Don't merge using calitp_url_number because ITP ID 282 (SFMTA)
            # can use calitp_url_number = 1
            # Just keep calitp_url_number = 0 from routelines_ddf
            trips.drop(columns = "calitp_url_number")[
                shape_id_cols + ["route_id", "direction_id"]],
            on = shape_id_cols,
            how = "inner",
        ).drop_duplicates(subset=["calitp_itp_id", "route_id", 
                                  "direction_id", 
                                  "route_length", "x", "y"])
        .drop(columns = ["x", "y"])
        .reset_index(drop=True)
    )
    
    return m1

    
def find_longest_route_shapes(
    merged_routelines_trips: dg.GeoDataFrame
) -> dg.GeoDataFrame: 
    """
    Sort in descending order by route_length
    Since there are short/long trips for a given route_id,
    Keep the longest shape_ids within a route_id-direction_id
    """
    route_dir_cols = ["calitp_itp_id", "calitp_url_number",
                      "route_id", "direction_id"]

    # Keep the longest shape_id for each direction
    longest_shape_by_direction = (
        merged_routelines_trips.groupby(route_dir_cols)
        .route_length.max().compute()
    ).reset_index()
    

    longest_shapes = dd.merge(
        merged_routelines_trips, 
        longest_shape_by_direction,
        on = route_dir_cols + ["route_length"],
        how = "inner"
    ).reset_index(drop=True)
    
    return longest_shapes


def symmetric_difference_by_route(longest_shapes: dg.GeoDataFrame, 
                                  route: str) -> gpd.GeoDataFrame:
    """
    For a given route, take the 2 longest shapes (1 for each direction),
    find the symmetric difference.
    
    Explode and only keep segments longer than 750 m.
    Concatenate this with the longest shape (1 direction) then dissolve
    
    Returns only 1 row for each route.
    Between the 2 directions, this is as full of a route network we can get
    without increasing complexity. 
    """
    route_cols = ["calitp_itp_id", "calitp_url_number", "route_id"]
    
    # Start with the longest direction (doesn't matter if it's 0 or 1)
    one_route = (longest_shapes[longest_shapes.route_id == route]
             .compute() # gpd.overlay requires gpd.GeoDataFrame
             # in case multiple rows are selected
             # only keep 1 for each direction (pick 1st shape_id)
             .sort_values(["direction_id", "shape_id", "route_length"], 
                          ascending=[True, True, False])
             .drop_duplicates(subset=["direction_id"])
             .reset_index(drop=True)
            )
    
    first = one_route[one_route.index==0]
    second = one_route[one_route.index==1]
    
    # Find the symmetric difference 
    # This takes away the parts that are overlapping
    # and keeps the differences -- this still isn't what we fully want, but gets closer
    overlay = first.overlay(second, how = "symmetric_difference")
    
    # Notice that overlay keeps a lot of short segments that are in the
    # middle of the route. Drop these. We mostly want
    # layover spots and where 1-way direction is.
    exploded = (overlay[["geometry"]].dissolve()
                .explode(index_parts=True)
                .reset_index()
                .drop(columns = ["level_0", "level_1"])
               )
    
    # Need to populate our exploded df with these
    itp_id = one_route.calitp_itp_id.iloc[0]
    url_number = one_route.calitp_url_number.iloc[0]
    
    exploded2 = exploded.assign(
        overlay_length = exploded.geometry.length,
        route_id = route,
        calitp_itp_id = itp_id,
        calitp_url_number = url_number
    )
    
    CUTOFF = 750 # 750 m is pretty close to how long our hqta segments are,
    # which are 1,250 m. Maybe these segments are long enough to be included.
    
    exploded_long_enough = exploded2[exploded2.overlay_length > CUTOFF]
    
    # Now, dissolve it, so it becomes 1 row again
    segments_to_attach = (exploded_long_enough[route_cols + ["geometry"]]
                          .dissolve(by=route_cols)
                          .reset_index()
                         )
    
    
    # Do this on the longest shape_id, because once it becomes
    # multilinestring, dask can't do the shapely Point(x.coords) operation
    # Grab the start / endpoint of a linestring
    #https://gis.stackexchange.com/questions/358584/how-to-extract-long-and-lat-of-start-and-end-points-to-seperate-columns-from-t
    first = first.assign(
        origin = first.geometry.apply(lambda x: Point(x.coords[0])),
        destination = first.geometry.apply(lambda x: Point(x.coords[-1])),
    )
    
    # Concatenate the longest one shape_id
    # with the segments that are long enough
    combined_longest = pd.concat([
        first[route_cols + ["origin", "destination", "geometry"]],
        segments_to_attach
    ], axis=0, ignore_index=True)
    
    combined = combined_longest.dissolve(by=route_cols).reset_index()
    
    return combined


def symmetric_difference_for_operator(longest_shapes: dg.GeoDataFrame
                                      ) -> gpd.GeoDataFrame:
    """
    Loop through each route and assemble the cleaned up symmetric
    difference overlays. 
    
    Overlay requires geopandas. After concatenating all the routes
    within an operator, turn it back to dask_geopandas.
    
    Returns a dg.GeoDataFrame.
    """
    operator_routes = list(longest_shapes.route_id.unique().compute())
    
    all_routes = gpd.GeoDataFrame()
    
    for r in sorted(operator_routes):
        one_route = symmetric_difference_by_route(longest_shapes, r)
        
        all_routes = pd.concat([all_routes, one_route], 
                               axis=0, ignore_index=True)
        
    longest_shape = dg.from_geopandas(all_routes, npartitions=1)
    
    return longest_shape
        

def add_route_cardinal_direction(
    df: dg.GeoDataFrame) -> dg.GeoDataFrame:
    """
    For each row, grab the origin/destination of the linestring.
    Put OD into rt_utils.primary_cardinal_direction(), 
    and aggregate "northbound", "southbound", etc to 
    to "north-south" or "east-west".
    """
    # Stick the origin/destination of a route_id and return the primary cardinal direction
    df = df.assign(
        route_primary_direction = df.apply(
            lambda x: rt_utils.primary_cardinal_direction(
                x.origin, x.destination), axis=1, meta=('route_direction', 'str'))
    )
    
    # Don't care exactly if it's southbound or northbound, but care that it's north-south
    # Want to test for orthogonality for 2 bus routes intersecting
    df = df.assign(
        route_direction = df.route_primary_direction.apply(
            lambda x: "north-south" if x in ["Northbound", "Southbound"]
            else "east-west", meta=('route_direction', 'str')
        ), 
    ).drop(columns = ["origin", "destination", "route_primary_direction"])

    return df
        

def select_needed_shapes_for_route_network(
    routelines: dg.GeoDataFrame, 
    trips: dd.DataFrame) -> gpd.GeoDataFrame:
    """
    Merge the routelines and trips tables,
    narrow down all the shape_ids to just 1 per route_id.
    
    Possible to expand to 2 per route_id if needed.
    
    Add cardinal direction for that route_id.

    Returns a gpd.GeoDataFrame, ready to be cut into hqta_segments
    """
    route_cols = ["calitp_itp_id", "calitp_url_number", "route_id"]

    # Merge routelines to trips, and drop shape_ids that are 
    # giving the same info (in terms of route_length, direction_id)
    merged = merge_routes_to_trips(routelines, trips)
    
    # Keep only the longest 2 shape_ids (1 in each direction) for each route_id
    longest_shapes = find_longest_route_shapes(merged)
    
    # Do a gpd.overlay to combine the 2 shape_ids into 1 row for each route_id
    longest_shape = symmetric_difference_for_operator(longest_shapes)
    
    longest_shape_with_dir = add_route_cardinal_direction(longest_shape).compute()
        
    return longest_shape_with_dir


def add_buffer(gdf: gpd.GeoDataFrame, 
               buffer_size: int = 50) -> gpd.GeoDataFrame:
    gdf = gdf.assign(
        geometry = gdf.geometry.buffer(buffer_size)
    )
    
    return gdf


def add_segment_id(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    ## compute (hopefully unique) hash of segment id that can be used 
    # across routes/operators
    df2 = df.assign(
        hqta_segment_id = df.apply(lambda x: 
                                   # this checksum hash always give same value if 
                                   # the same combination of strings are given
                                   zlib.crc32(
                                       (str(x.calitp_itp_id) + 
                                        x.route_id + x.segment_sequence)
                                       .encode("utf-8")), 
                                       axis=1),
    )

    return df2


def segment_route(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Draw segments every 1,250 m.
    Then create an hqta_segment_id that we can reference later on.
    """
    segmented = gpd.GeoDataFrame() 

    route_cols = ["calitp_itp_id", "calitp_url_number", "route_id"]

    # Turn 1 row of geometry into hqta_segments, every 1,250 m
    # create_segments() must take a row.geometry (gpd.GeoDataFrame)
    for segment in utilities.create_segments(gdf.geometry):
        to_append = gdf.drop(columns=["geometry"])
        to_append["geometry"] = segment
        segmented = pd.concat([segmented, to_append], axis=0, ignore_index=True)
    
        segmented = segmented.assign(
            temp_index = (segmented.sort_values(route_cols)
                          .reset_index(drop=True).index
                         )
        )
    
    segmented = (segmented.assign(
        segment_sequence = (segmented.groupby(route_cols)["temp_index"]
                            .transform("rank") - 1).astype(int).astype(str)
                           )
                 .sort_values(route_cols)
                 .reset_index(drop=True)
                 .drop(columns = "temp_index")
                )
    
    return segmented
        
    
