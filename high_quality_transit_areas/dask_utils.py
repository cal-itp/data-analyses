import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import numpy as np
import pandas as pd
import zlib

from shapely.geometry import LineString, Point

import utilities
from shared_utils import rt_utils


#------------------------------------------------------#
# Stop times
#------------------------------------------------------#
## Aggregate stops by departure hour
def fix_departure_time(stop_times):
    # Some fixing, transformation, aggregation with dask
    # Grab departure hour
    #https://stackoverflow.com/questions/45428292/how-to-convert-pandas-str-split-call-to-to-dask
    stop_times2 = stop_times[~stop_times.departure_time.isna()].reset_index(drop=True)
    
    ddf = stop_times2.assign(
        departure_hour = stop_times2.departure_time.str.partition(":")[0].astype(int)
    )
    
    # Since hours past 24 are allowed for overnight trips
    # coerce these to fall between 0-23
    #https://stackoverflow.com/questions/54955833/apply-a-lambda-function-to-a-dask-dataframe
    ddf["departure_hour"] = ddf.departure_hour.map(lambda x: x-24 if x >=24 else x)
    
    return ddf
    
def stop_times_aggregation_by_hour(stop_times):
    stop_cols = ["calitp_itp_id", "stop_id"]

    ddf = fix_departure_time(stop_times)
    
    # Aggregate how many trips are made at that stop by departure hour
    trips_per_hour = (ddf.groupby(stop_cols + ["departure_hour"])
                      .agg({'trip_id': 'count'})
                      .reset_index()
                      .rename(columns = {"trip_id": "n_trips"})
                     )    
    
    return trips_per_hour


# utilities.find_stop_with_high_trip_count
def find_stop_with_high_trip_count(stop_times): 
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
# For LA Metro, out of ~700 unique shape_ids,
# this pares is down to ~115 route_ids
# Use the pared down shape_ids to get hqta_segments
def merge_routes_to_trips(routelines, trips):    
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
                shape_id_cols + ["route_id"]],
            on = shape_id_cols,
            how = "inner",
        ).drop_duplicates(subset=["calitp_itp_id", "route_id", 
                                  "route_length", "x", "y"])
        .drop(columns = ["x", "y"])
        .reset_index(drop=True)
    )
    
    return m1

    
def find_longest_route_shapes(merged_routelines_trips, n=1): 
    # Sort in descending order by route_length
    # Since there are short/long trips for a given route_id,
    # Keep the longest 5 shape_ids within a route_id 
    # then do the dissolve
    # Doing it off of the full one creates gaps in the line geom
    df = merged_routelines_trips.assign(
        obs = (merged_routelines_trips.sort_values(["route_id", "route_length"], 
                                  ascending=[True, False])
               .groupby(["calitp_itp_id", "route_id"]).cumcount() + 1
              )
    )
    
    longest_shape = df[df.obs <= n].drop(columns="obs").reset_index(drop=True)
    
    return longest_shape


def add_route_cardinal_direction(df):
    # Grab the start / endpoint of a linestring
    #https://gis.stackexchange.com/questions/358584/how-to-extract-long-and-lat-of-start-and-end-points-to-seperate-columns-from-t
    df = df.assign(
        origin = df.geometry.apply(lambda x: Point(x.coords[0]), 
                                              meta=('origin', 'geometry')),
        destination = df.geometry.apply(lambda x: Point(x.coords[-1]), 
                                                   meta=('destination', 'geometry')),
    )
    
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
        

def select_needed_shapes_for_route_network(routelines, trips):
    route_cols = ["calitp_itp_id", "calitp_url_number", "route_id"]

    # For a given route, the longest route_length may provide 90% of the needed shape 
    # (line geom)
    # But, it's missing parts where buses may turn around for layovers
    # Add these segments in, so we can build out a complete route network
    merged = merge_routes_to_trips(routelines, trips)
    
    # Go back to picking the longest one (n=1), since n=5 gives errors
    # Suspect that the dissolve/unary_union orders the points differently, 
    # and the hqta segments are truncated way too short
    longest_shape = find_longest_route_shapes(merged)
    
    # CHANGE GEOMETRY?
    # Can either keep the shape_id and associate the dissolved geometry with that shape_id
    # Or, drop shape_id, since now shape_id is not reflecting the raw line geom 
    # for that shape_id, and that's confusing to the end user (also, shape_id is not used, since hqta_segment_id is primary unit of analysis)
    '''
    dissolved_by_route = (longest_shape[route_cols + ["geometry"]]
                          .compute()
                          .dissolve(by=route_cols)
                          .reset_index()
                         )
    '''
    
    longest_shape_with_dir = add_route_cardinal_direction(longest_shape).compute()
        
    return longest_shape_with_dir


def add_buffer(gdf, buffer_size=50):
    gdf = gdf.assign(
        geometry = gdf.geometry.buffer(buffer_size)
    )
    
    return gdf


def add_segment_id(df):
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


def segment_route(gdf):
    segmented = gpd.GeoDataFrame() ##changed to gdf?

    route_cols = ["calitp_itp_id", "calitp_url_number", "route_id"]

    # Turn 1 row of geometry into hqta_segments, every 1,250 m
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
        
    
