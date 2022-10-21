import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import zlib

from shared_utils import gtfs_utils, geography_utils, rt_utils, utils
from update_vars import analysis_date, COMPILED_CACHED_VIEWS

DASK_GCS = "gs://calitp-analytics-data/data-analyses/dask_test/"

HQTA_SEGMENT_LENGTH = 1_250 # meters

## These should all be functions that handle everything that can be done at once
# Put in corridor_utils?
def max_trips_by_group(df: dd.DataFrame, 
                       group_cols: list,
                       max_col: str = "n_trips"
                      ) -> dd.DataFrame:
    """
    Find the max trips, by stop_id or by hqta_segment_id.
    Put in a list of group_cols to find the max.
    Can also subset for AM or PM by df[df.departure_hour < 12]
    """
    df2 = (df.groupby(group_cols)
           .agg({max_col: np.max})
           .reset_index()
          )
    
    return df2 


def stop_times_aggregation_max_by_stop(stop_times: dd.DataFrame) -> dd.DataFrame:
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
    
    # Subset to departure hour before or after 12pm
    am_trips = max_trips_by_group(
        trips_per_hour[trips_per_hour.departure_hour < 12], 
        group_cols = stop_cols,
        max_col = "n_trips"
    ).rename(columns = {"n_trips": "am_max_trips"})
    
    pm_trips = max_trips_by_group(
        trips_per_hour[trips_per_hour.departure_hour >= 12], 
        group_cols = stop_cols,
        max_col = "n_trips"
    ).rename(columns = {"n_trips": "pm_max_trips"})
    
    max_trips_by_stop = dd.merge(
        am_trips, 
        pm_trips,
        on = stop_cols,
        how = "left"
    )
    
    max_trips_by_stop = max_trips_by_stop.assign(
        am_max_trips = max_trips_by_stop.am_max_trips.fillna(0).astype(int),
        pm_max_trips = max_trips_by_stop.pm_max_trips.fillna(0).astype(int),
        n_trips = (max_trips_by_stop.am_max_trips.fillna(0) + 
                   max_trips_by_stop.pm_max_trips.fillna(0))
    )
        
    return max_trips_by_stop


# was in corridor_utils

def merge_routes_to_trips(routelines: dg.GeoDataFrame, 
                          trips: dd.DataFrame) -> dg.GeoDataFrame:   
    """
    Merge routes and trips tables.
    Keep the longest shape by route_length in each direction.
    
    For LA Metro, out of ~700 unique shape_ids,
    this pares it down to ~115 route_ids.
    Use this pared down shape_ids to get hqta_segments.
    """
    shape_id_cols = ["calitp_itp_id", "shape_id"]
    route_dir_cols = ["calitp_itp_id", "route_id", "direction_id"]

    routelines_ddf = routelines.assign(
        route_length = routelines.geometry.length,
    )
        
    # Merge routes to trips with using trip_id
    # Keep route_id and shape_id, but drop trip_id by the end
    m1 = (dd.merge(
            routelines_ddf,
            # Don't merge using calitp_url_number because ITP ID 282 (SFMTA)
            # can use calitp_url_number = 1
            # Just keep calitp_url_number = 0 from routelines_ddf
            trips[shape_id_cols + ["route_id", "direction_id"]],
            on = shape_id_cols,
            how = "inner",
        ).drop_duplicates(subset = route_dir_cols + ["route_length"])
        .reset_index(drop=True)
    )
    
    # If direction_id is missing, then later code will break, because
    # we need to find the longest route_length
    # Don't really care what direction is, since we will replace it with north-south
    # Just need a value to stand-in, treat it as the same direction
    m1 = m1.assign(
        direction_id = m1.direction_id.fillna('0')
    )
    
    m1 = m1.assign(    
        # dask can only sort by 1 column
        # so, let's combine route-dir into 1 column and drop_duplicates
        route_dir_identifier = m1.apply(
            lambda x: zlib.crc32(
                (str(x.calitp_itp_id) + 
                x.route_id + str(x.direction_id))
            .encode("utf-8")), axis=1, 
            meta=("route_dir_identifier", "int"))
    )
    
    # Keep the longest shape_id for each direction
    # with missing direction_id filled in
    longest_shapes = (m1.sort_values("shape_id")
                      .drop_duplicates("route_dir_identifier")
                      .drop(columns = "route_dir_identifier")
                     )

    # Let's add a route-identifier...to use downstream as partitioning index?
    # Do this on the longest shape_id, because once it is dissolved, becomes multi-part geom
    # dask can't do the shapely Point(x.coords) operation
    # Grab the start / endpoint of a linestring
    #https://gis.stackexchange.com/questions/358584/how-to-extract-long-and-lat-of-start-and-end-points-to-seperate-columns-from-t
    longest_shapes = longest_shapes.assign(    
        route_identifier = longest_shapes.apply(
            lambda x: zlib.crc32(
                (str(x.calitp_itp_id) + x.route_id)
            .encode("utf-8")), axis=1, 
            meta=("route_identifier", "int")),
    )
        
    return longest_shapes


def symmetric_difference_by_route(longest_shapes: gpd.GeoDataFrame, 
                                  route: str, 
                                  segment_length: int
                                 ) -> gpd.GeoDataFrame:
    """
    For a given route, take the 2 longest shapes (1 for each direction),
    find the symmetric difference.
    
    Explode and only keep segments longer than 750 m.
    Concatenate this with the longest shape (1 direction) then dissolve
    
    Returns only 1 row for each route.
    Between the 2 directions, this is as full of a route network we can get
    without increasing complexity. 
    """    
    # Start with the longest direction (doesn't matter if it's 0 or 1)
    one_route = (longest_shapes[longest_shapes.route_identifier == route]
                 .sort_values("route_length", ascending=False)
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
    
    exploded2 = exploded.assign(
        overlay_length = exploded.geometry.length,
        route_identifier = route,
    )
    
    CUTOFF = segment_length * 0.5
    # 750 m is pretty close to how long our hqta segments are,
    # which are 1,250 m. Maybe these segments are long enough to be included.
    
    exploded_long_enough = exploded2[exploded2.overlay_length > CUTOFF]
   
    # Now, dissolve it, so it becomes 1 row again
    # Without this initial dissolve, hqta segments will have tiny segments towards ends
    segments_to_attach = (exploded_long_enough[["route_identifier", "geometry"]]
                          .dissolve(by="route_identifier")
                          .reset_index()
                         )

    # Now, combine the segments to attach with the longest shape in one direction
    # this gives us full route network
    # dissolve it, so it becomes 1 row at route-level (no longer direction dependent)
    combined = (pd.concat(
                [first, segments_to_attach], axis=0)
                [["route_identifier", "geometry"]]
                .dissolve(by="route_identifier")
                .reset_index()
               )

    return combined  


def dissolve_two_directions_into_one(
    longest_shapes: dg.GeoDataFrame, 
    segment_length: int) -> gpd.GeoDataFrame:
    """
    If 2 observations are kept, 1 in each direction,
    take the symmetric difference.
    
    Handle these separately, and allow routes to skip this
    if only 1 direction is present anyway (because longest route_length, 
    drop duplicates led to only 1 obs left)
    """
    # Since the symmetric difference needs to be geopandas,
    # convert it now, and leave it as geopandas
    longest_shapes = longest_shapes.compute()
        
    routes_with_both_directions = (longest_shapes.route_identifier
                                   .value_counts()
                                   .loc[lambda x: x > 1]
                                   .index).tolist()  
    
    one_direction = longest_shapes[~
        longest_shapes.route_identifier.isin(routes_with_both_directions)]
    
    two_directions = longest_shapes[
        longest_shapes.route_identifier.isin(routes_with_both_directions)]
    
    two_directions_dissolved = gpd.GeoDataFrame()

    for route in routes_with_both_directions:
        exploded = symmetric_difference_by_route(two_directions, 
                                                 route, segment_length)
    
        two_directions_dissolved = pd.concat(
            [two_directions_dissolved, exploded], axis=0)    
    
    route_cols = ["calitp_itp_id", "route_id", "route_identifier"]
    
    two_directions_dissolved2 = pd.merge(
        # dissolved supplies the geometry needed, put on left
        two_directions_dissolved,
        two_directions[route_cols].drop_duplicates(subset="route_identifier"),
        on = "route_identifier",
        how = "inner",
        validate = "1:1" 
    )
    
    longest_shape = (pd.concat([
        one_direction[route_cols + ["geometry"]], 
        two_directions_dissolved2[route_cols + ["geometry"]]], axis=0)
        .sort_values(["calitp_itp_id", "route_id"])
        .reset_index(drop=True)
    )
        
    return longest_shape


def find_primary_direction_across_hqta_segments(
    hqta_segments_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    
    # Get predominant direction based on segments
    predominant_direction_by_route = (
        hqta_segments_gdf.groupby(["route_identifier", "route_direction"])
        .agg({"route_primary_direction": "count"})
        .reset_index()
        .sort_values(["route_identifier", "route_primary_direction"], 
        # descending order, the one with most counts at the top
        ascending=[True, False])
        .drop_duplicates(subset="route_identifier")
        .reset_index(drop=True)
        [["route_identifier", "route_direction"]]
     )
    
    drop_cols = ["origin", "destination", "route_primary_direction"]
    
    routes_with_primary_direction = pd.merge(
        hqta_segments_gdf.drop(columns = "route_direction"), 
        predominant_direction_by_route,
        on = "route_identifier",
        how = "left",
        validate = "m:1"
    ).drop(columns = drop_cols)
    
    return routes_with_primary_direction
    

    
def select_longest_shapes_for_route_network(
    analysis_date: str, segment_length: int = HQTA_SEGMENT_LENGTH
) -> gpd.GeoDataFrame:
    
    routelines = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet")
    trips = dd.read_parquet(f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")

    # Keep longest shape in each direction
    longest_shapes = merge_routes_to_trips(routelines, trips)
    
    # Do a symmetric overlay, then dissolve
    # so that there's only 1 line geom for each route, and it captures both directions
    # symmetric overlay needed because otherwise hqta segments will be cut too tiny
    longest_shape = dissolve_two_directions_into_one(longest_shapes, segment_length)
    
    return longest_shape


def cut_route_network_into_hqta_segments(
    gdf: gpd.GeoDataFrame, segment_length: int = HQTA_SEGMENT_LENGTH
) -> gpd.GeoDataFrame:
    # HQTA segments are 1,250 m long
    hqta_segments = geography_utils.cut_segments(
        gdf,
        group_cols = ["calitp_itp_id", "route_id", "route_identifier"],
        segment_distance = segment_length
    )
    
    # compute (hopefully unique) hash of segment id that can be used
    # across routes/operators
    # this checksum hash always give same value if the same combo of strings are given
    hqta_segments = hqta_segments.assign(
        hqta_segment_id = hqta_segments.apply(
            lambda x: zlib.crc32(
                (str(x.calitp_itp_id) + x.route_id + 
                 x.segment_sequence).encode("utf-8")), axis=1),
    )
    
    with_od = rt_utils.add_origin_destination(hqta_segments)
    with_direction = rt_utils.add_route_cardinal_direction(with_od)
    
    # Since route_direction at the route-level could yield both north-south and east-west 
    # for a given route, use the segments to determine the primary direction
    hqta_segments2 = find_primary_direction_across_hqta_segments(with_direction)
    
    # Re-order columns and put geometry last
    cols = list(hqta_segments2.columns)
    hqta_segments2 = hqta_segments2.reindex(columns = cols[1:] + ["geometry"])
    
    return hqta_segments2


## Join HQTA segment to stop
def hqta_segment_to_stop(hqta_segments: dg.GeoDataFrame, 
                         stops: dg.GeoDataFrame
                        ) -> dg.GeoDataFrame:    
    segment_cols = ["hqta_segment_id", "segment_sequence"]

    segment_to_stop = (dg.sjoin(
            stops[["stop_id", "geometry"]],
            hqta_segments,
            how = "inner",
            predicate = "intersects"
        ).drop(columns = ["index_right"])
    )[segment_cols + ["stop_id"]]
    
    
    # After sjoin, we don't want to keep stop's point geom
    # Merge on hqta_segment_id's polygon geom
    segment_to_stop2 = dd.merge(
        hqta_segments,
        segment_to_stop,
        on = segment_cols
    )
    
    return segment_to_stop2


def hqta_segment_keep_one_stop(
    hqta_segments: dg.GeoDataFrame, 
    stop_times: pd.DataFrame) -> gpd.GeoDataFrame:
    
    stop_cols = ["calitp_itp_id", "stop_id"]
    # Keep the stop in the segment with highest trips (sum across AM and PM)
    # dd.merge between dask dataframes can be expensive
    # put pd.DataFrame on right if possible
    segment_to_stop_times = dd.merge(
            hqta_segments, 
            stop_times,
            on = stop_cols
        )
                      
    # Can't sort by multiple columns in dask,
    # so, find the max, then inner merge
    max_trips_by_segment = max_trips_by_group(
        segment_to_stop_times,
        group_cols = ["hqta_segment_id"],
        max_col = "n_trips"
    ).compute()
    
    # Merge in and keep max trips observation
    # Since there might be duplicates still, where multiple stops all 
    # share 2 trips for that segment, do a drop duplicates at the end 
    segment_to_stop_unique = dd.merge(
        segment_to_stop_times,
        max_trips_by_segment,
        on = ["hqta_segment_id", "n_trips"],
        how = "inner"
    ).drop_duplicates(subset=["hqta_segment_id", "am_max_trips", "pm_max_trips"])
    
    # In the case of same number of trips overall, do a sort
    # with descending order for AM, then PM trips
    segment_to_stop_gdf = segment_to_stop_unique.compute()
    segment_to_stop_gdf = (segment_to_stop_gdf
                           .sort_values(["hqta_segment_id",
                                         "am_max_trips", "pm_max_trips"], 
                                        ascending=[True, False, False])
                            .drop_duplicates(subset="hqta_segment_id")
                           .reset_index(drop=True)
                          )
    
    return segment_to_stop_gdf



def sjoin_stops_and_stop_times_to_hqta_segments(
    hqta_segments: gpd.GeoDataFrame | dg.GeoDataFrame, 
    stops: gpd.GeoDataFrame | dg.GeoDataFrame,
    stop_times: pd.DataFrame | dd.DataFrame,
    buffer_size: int = 50,
    hq_transit_threshold: int = 4,
) -> dg.GeoDataFrame:
        
    # Draw 50 m buffer to capture stops around hqta segments
    hqta_segments2 = hqta_segments.assign(
        geometry = hqta_segments.geometry.buffer(buffer_size)
    )
    
    # Join hqta segment to stops
    segment_to_stop = hqta_segment_to_stop(hqta_segments2, stops)
    
    segment_to_stop_unique = hqta_segment_keep_one_stop(
        segment_to_stop, stop_times)

    # Identify hq transit corridor
    # Tag segment as being hq_transit_corr if it has at least 4 trips in AM and PM 
    # (before 12pm, after 12pm, whatever is max in each period)
    drop_cols = ["n_trips"]

    segment_hq_corr = segment_to_stop_unique.assign(
        hq_transit_corr = segment_to_stop_unique.apply(
            lambda x: True if (x.am_max_trips > hq_transit_threshold and 
                               (x.pm_max_trips > hq_transit_threshold))
            else False, axis=1)
    ).drop(columns = drop_cols)
    

    return segment_hq_corr
    

if __name__=="__main__":
    '''

    # (1) Aggregate stop times - by stop_id, find max trips in AM/PM peak
    # takes 1 min
    stop_times = dd.read_parquet(f"{COMPILED_CACHED_VIEWS}st_{analysis_date}.parquet")
    max_arrivals_by_stop = stop_times_aggregation_max_by_stop(stop_times)
    
    max_arrivals_by_stop.compute().to_parquet(f"{DASK_GCS}max_arrivals_by_stop.parquet")
    
    time1 = datetime.datetime.now()
    print(f"stop time aggregation saved to GCS: {time1 - start}")
    '''
    time1 = datetime.datetime.now()
    
    # (2) Grab longest shapes for route network
    # Only longest shape in each direction -> dissolved into 1 line geom per route
    # Takes <7 min
    longest_shape = select_longest_shapes_for_route_network(analysis_date, 
                                                            HQTA_SEGMENT_LENGTH) 
    
    utils.geoparquet_gcs_export(longest_shape,
                                DASK_GCS,
                                "longest_shape_with_dir"
                               )
    
    time2 = datetime.datetime.now()
    print(f"routelines dissolved saved to GCS: {time2 - time1}")
    
    # (3) Cut route network into hqta segments, add route_direction
    # Takes 23 min to run
    routes = gpd.read_parquet(f"{DASK_GCS}longest_shape_with_dir.parquet")
    
    hqta_segments = cut_route_network_into_hqta_segments(routes, HQTA_SEGMENT_LENGTH)

    utils.geoparquet_gcs_export(hqta_segments, 
                                DASK_GCS,
                                "hqta_segments"
                               )
    time3 = datetime.datetime.now()
    print(f"cut segments: {time3 - time2}")
    
    ## (4) Spatial join stops and stop times to hqta segments
    # this takes < 2 min
    hqta_segments = dg.read_parquet(f"{DASK_GCS}hqta_segments.parquet")
    stops = dg.read_parquet(f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet")
    max_arrivals_by_stop = pd.read_parquet(f"{DASK_GCS}max_arrivals_by_stop.parquet") 
    
    hqta_corr = sjoin_stops_and_stop_times_to_hqta_segments(
        hqta_segments, 
        stops,
        max_arrivals_by_stop,
        buffer_size = 50, #50meters
        hq_transit_threshold = 4
    )
        
    hqta_corr.to_parquet("./all_bus.parquet")
    utils.geoparquet_gcs_export(
        hqta_corr,
        DASK_GCS,
        "all_bus"
    )
     
    end = datetime.datetime.now()
    print(f"sjoin stops and stop times: {end-time3}")
    print(f"execution: {end-time1}")