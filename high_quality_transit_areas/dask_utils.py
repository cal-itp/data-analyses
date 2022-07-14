import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import numpy as np
import pandas as pd
import zlib

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
        route_length = routelines.geometry.length
    )
        
    # Merge routes to trips with using trip_id
    # Keep route_id and shape_id, but drop trip_id by the end
    shape_id_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"]
    
    m1 = (dd.merge(
            routelines_ddf,
            trips[shape_id_cols + ["trip_id", "route_id"]],
            on = shape_id_cols,
            how = "left",
        ).drop_duplicates(subset=shape_id_cols)
        .reset_index(drop=True)
    )
    
    return m1


def find_longest_route_shape(merged_routelines_trips):
    # Sort in descending order by route_length
    # Since there are short/long trips for a given route_id,
    # Keep the longest one for each route_id
    longest_shape = (merged_routelines_trips.drop(columns = "trip_id")
                     .sort_values(["route_id", "route_length"],
                     ascending=[True, False])
      .drop_duplicates(subset="route_id")
      .reset_index(drop=True)
     ).compute()
    
    return longest_shape



def overlay_longest_shape_with_other_shapes(longest_shape, other_shapes):    
    overlay_diff = gpd.overlay(
        other_shapes,
        longest_shape[["shape_id", "geometry"]],
        how = "difference",
        # False keeps all geometries
        keep_geom_type=False
    )
    
    overlay_diff_gddf = dask_geopandas.from_geopandas(
        overlay_diff[overlay_diff.route_id.notna()], npartitions=1)
    
    # Once you overlay the other shapes against the longest route
    # calculate the overlay length
    # Go from longest length and add that shape_id to the longest route
    # The longest overlay length will help us add more of the physical route network that's missing
    
    # From these differences, there's several segments that duplicative
    # But, doing it with gpd.overlay isn't good at sorting it
    # Find the centroid of these smaller segments, and if the centroid is the same, drop the duplicate
    overlay_diff_gddf = overlay_diff_gddf.assign(
        overlay_length = overlay_diff_gddf.geometry.length,
        x = overlay_diff_gddf.geometry.centroid.x.round(3),
        y = overlay_diff_gddf.geometry.centroid.y.round(3),
    )
    
    route_cols = ["calitp_itp_id", "calitp_url_number", "route_id"]

    overlay_diff_gddf2 = (overlay_diff_gddf
                          .drop_duplicates(subset = route_cols + ["x", "y"])
                          .reset_index(drop=True)
                         ).compute()
    
    return overlay_diff_gddf2


def select_needed_shapes_for_route_network(routelines, trips):
    route_cols = ["calitp_itp_id", "calitp_url_number", "route_id"]

    # For a given route, the longest route_length may provide 90% of the needed shape (line geom)
    # But, it's missing parts where buses may turn around for layovers
    # Add these segments in, based on those shape_ids, so we can build out a complete route network
    merged = merge_routes_to_trips(routelines, trips)
    
    longest_shape = find_longest_route_shape(merged)
    
    '''
    # Not the longest routes
    other_shapes = (merged[~merged.shape_id.isin(longest_shape.shape_id)]
          .sort_values(route_cols + ["route_length"], 
                          ascending=[True, True, True, False])
          .reset_index(drop=True)
         ).compute()

    
    # For all the other shapes that are not the longest route_length
    # Find the difference using gpd.overlay when compared to the longest shape
    overlay_diff = overlay_longest_shape_with_other_shapes(
        longest_shape, other_shapes)
    
    keep_cols = route_cols + ["shape_id", "geometry"]

    expanded_shape = pd.concat([longest_shape[keep_cols], 
                                 overlay_diff[keep_cols]], axis=0)
    
    expanded_shape = (expanded_shape.assign(
                        route_length = expanded_shape.geometry.length
                    ).sort_values(route_cols + ["route_length"], 
                                  ascending=[True, True, True, False])
                      .reset_index(drop=True)
                     )
    
    return expanded_shape
    '''
    return longest_shape


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
                                       (str(x.calitp_itp_id) + x.shape_id + x.segment_sequence)
                                       .encode("utf-8")), 
                                       axis=1),
    )

    return df2


def segment_route(gdf):
    segmented = gpd.GeoDataFrame() ##changed to gdf?

    route_shape_cols = ["calitp_itp_id", "calitp_url_number", 
                  "route_id", "shape_id"]

    # Turn 1 row of geometry into hqta_segments, every 1,250 m
    for segment in utilities.create_segments(gdf.geometry):
        to_append = gdf.drop(columns=["geometry"])
        to_append["geometry"] = segment
        segmented = pd.concat([segmented, to_append], axis=0, ignore_index=True)
    
        segmented = segmented.assign(
            temp_index = (segmented.sort_values(route_shape_cols)
                          .reset_index(drop=True).index
                         )
        )
    
    segmented = (segmented.assign(
        segment_sequence = (segmented.groupby(route_shape_cols)["temp_index"]
                            .transform("rank") - 1).astype(int).astype(str)
                           )
                 .sort_values(route_shape_cols)
                 .reset_index(drop=True)
                 .drop(columns = "temp_index")
                )
    
    return segmented
        
    
