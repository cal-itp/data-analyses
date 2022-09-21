"""
Compile highway segment level stats for visualizations.
"""
import dask.dataframe as dd
import intake
import geopandas as gpd
import pandas as pd

import E2_aggregated_route_stats as aggregated_route_stats
from shared_utils import geography_utils, utils
from E0_bus_oppor_vars import GCS_FILE_PATH, ANALYSIS_DATE, COMPILED_CACHED_GCS

catalog = intake.open_catalog("*.yml")

highway_cols = ["District", "County", "Route", "RouteType", "hwy_segment_id"]
route_cols = ["calitp_itp_id", "route_id"]
route_segment_cols = route_cols + ["hwy_segment_id"]


def draw_buffer(gdf: gpd.GeoDataFrame, buffer: int = 50):
    gdf2 = gdf.to_crs(geography_utils.CA_StatePlane)
    
    gdf_poly = gdf2.assign(
        geometry = gdf2.geometry.buffer(buffer)
    )
    
    return gdf_poly


def sjoin_bus_routes_to_hwy_segments(highways: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Highways segments are for all CA.
    Bus routes contains the routes where at least 50% of route runs on SHN.
    """
    bus_routes = catalog.bus_routes_on_hwys.read().rename(
        columns = {"itp_id": "calitp_itp_id"})
    
    # Spatial join to find the highway segments with buses that run on SHN  
    # Highways is polygon, bus_routes is linestring 
    # Put highways on left because that's the geom I want to keep
    s1 = gpd.sjoin(
        highways[highway_cols + ["geometry"]].drop_duplicates(),
        bus_routes[route_cols + ["geometry"]].drop_duplicates(),
        how = "inner",
        predicate="intersects"
    ).drop(columns = "index_right")
    
    return s1 


def average_speed_by_stop():   
    # Stop-level speeds
    # Can take straight average, since each trip has equal weight?
    # There's no distance metric when it's a snapshot of a speed at a location
    speed_by_stops = catalog.speeds_by_stop.read()
        
    # Drop observations with way too fast speeds
    speed_by_stops2 = speed_by_stops[speed_by_stops.speed_mph <= 65]
    
    mean_speed = geography_utils.aggregate_by_geography(
        speed_by_stops2,
        group_cols = ["calitp_itp_id", "stop_id"],
        mean_cols = ["speed_mph"],
        nunique_cols = ["trip_id"]
    ).rename(columns = {"speed_mph": "mean_speed_mph", 
                        "trip_id": "num_trips"}
            ).astype({"num_trips": "Int64"})
    
    return mean_speed
        

def sjoin_stops_to_highway_segments(
    highways: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Spatial join highway segments polygon to stops.
    """
    stops = gpd.read_parquet(f"{COMPILED_CACHED_GCS}stops_{ANALYSIS_DATE}.parquet")

    stops_on_hwys = gpd.sjoin(
        stops.to_crs(geography_utils.CA_StatePlane), 
        highways.to_crs(geography_utils.CA_StatePlane)[["hwy_segment_id", "geometry"]],
        how = "inner",
        predicate="intersects"
    ).drop(columns = "index_right")

    # No duplicates of same stop_id by hwy segment
    stops_on_hwys2 = (stops_on_hwys.drop_duplicates(
        subset=["calitp_itp_id", "stop_id", "hwy_segment_id"]
        ).reset_index(drop=True)
    )
    
    # Merge in RT speed info by stop-level
    mean_speed_by_stop = average_speed_by_stop()
    
    stops_with_speeds = pd.merge(
        stops_on_hwys2, 
        mean_speed_by_stop,    
        on = ["calitp_itp_id", "stop_id"],
        how = "inner",
    )
    
    return stops_with_speeds


def sjoin_stops_to_highway_segments_get_stop_times(
    highways: gpd.GeoDataFrame) -> dd.DataFrame:
    """
    Spatial join highway segments polygon to stops
    and merge in stop_times info for these stops
    """    
    stops_with_speeds = sjoin_stops_to_highway_segments(highways)
    
    # Join in stop_times for stops for routes on SHN
    # Use the filtered stop times for stops that do get attached to hwy_segments    
    stop_times_with_hr = catalog.stop_times_for_routes_on_shn.read()
    stop_times_on_hwy = pd.merge(
            stop_times_with_hr, 
            stops_with_speeds[["calitp_itp_id", "stop_id", "hwy_segment_id"]],
            on = ["calitp_itp_id", "stop_id"],
            how = "inner"
        )
    
    # Turn into dask df to put into aggregation function
    stop_times_on_hwy = dd.from_pandas(stop_times_on_hwy, npartitions=1)
    
    return stop_times_on_hwy


def calculate_trip_weighted_speed(gdf: gpd.GeoDataFrame, 
                                  group_cols: list) -> pd.DataFrame:
    """
    Speed (mph) should be weighted by number of trips.
    Generate the weighted average separately, easier to merge in later.
    """
    segment_speed = gdf.assign(
        weighted_speed = gdf.mean_speed_mph * gdf.num_trips
    )
    
    segment_speed_agg = geography_utils.aggregate_by_geography(
        segment_speed,
        group_cols = group_cols,
        sum_cols = ["weighted_speed", "num_trips"]
    )
    
    segment_speed_agg = segment_speed_agg.assign(
        mean_speed_mph_trip_weighted = (segment_speed_agg.weighted_speed / 
                                        segment_speed_agg.num_trips
                                       )
    ).drop(columns = ["weighted_speed", "num_trips"])
    
    return segment_speed_agg


def aggregate_to_hwy_segment(df: pd.DataFrame, 
                             highway_segment_gdf: gpd.GeoDataFrame,
                             group_cols: list) -> gpd.GeoDataFrame:
    """
    Aggregate df to hwy segment.
    Attach hwy segment polygon geometry back on.
    """
    sum_cols = [
        'trips_peak', 'trips_all_day', 
        'stop_arrivals_peak', 'stop_arrivals_all_day',
        'stops_peak', 'stops_all_day',
    ]
    
    # These are stats we can easily sum up, to highway segment level
    segment = geography_utils.aggregate_by_geography(
        df,
        group_cols = group_cols,
        sum_cols = sum_cols,
    )
    
    # Attach the highway segment line geom back in
    other_hwy_cols = list(set(highway_cols).difference(set(["hwy_segment_id"])))
    
    segment_with_geom = geography_utils.attach_geometry(
        segment,
        highway_segment_gdf[group_cols + other_hwy_cols + 
                            ["geometry"]].drop_duplicates(),
        merge_col = group_cols,
        join = "inner"
    )
    
    # Clean up dtypes, re-order columns
    segment_with_geom[sum_cols] = segment_with_geom[sum_cols].astype(int)
    
    segment_with_geom = segment_with_geom.reindex(
        columns = other_hwy_cols + group_cols + sum_cols + ["geometry"])
    
    return segment_with_geom


def build_highway_segment_table(segment_stats: pd.DataFrame,
                                highway_segments: gpd.GeoDataFrame,
                                highway_polygons: gpd.GeoDataFrame,
                               ) -> gpd.GeoDataFrame:

    segment_stats1 = aggregate_to_hwy_segment(
        segment_stats, highway_segments, ["hwy_segment_id"])

    # Add in average speed, aggregated to hwy_segment_id
    stops_on_hwy = sjoin_stops_to_highway_segments(highway_polygons)
    
    # Note: speeds are weighted for trips present in GTFS schedule
    # not for GTFS RT. GTFS RT speeds are a sample, and it may not 
    # reflect 100% of all scheduled trips
    weighted_speeds = calculate_trip_weighted_speed(
        stops_on_hwy, ["hwy_segment_id"])
    
    segment_stats2 = pd.merge(
        segment_stats1,
        weighted_speeds,
        on = "hwy_segment_id",
        how = "left",
        validate = "1:1"
    )
    
    hwy_cols_except_id = [c for c in highway_cols if c != 'hwy_segment_id']
    
    # Merge in geometry for highway segments
    gdf = pd.merge(
        highways, 
        segment_stats2.drop(columns = hwy_cols_except_id + ["geometry"]), 
        on = "hwy_segment_id",
        how = "left",
        validate = "1:1"
    )
    
    # Clean up columns by filling in missing values with 0's
    exclude_me = ["segment_sequence", "geometry", "mean_speed_mph_trip_weighted", ]
    integrify_me = [c for c in gdf.columns if c not in highway_cols and 
                   c not in exclude_me]
    
    gdf[integrify_me] = gdf[integrify_me].fillna(0).astype(int)
    gdf["mean_speed_mph_trip_weighted"] = gdf.mean_speed_mph_trip_weighted.round(2)
    
    # Add per mi metrics
    gdf2 = normalize_stats(gdf, integrify_me)
    
    return gdf2


def normalize_stats(df: gpd.GeoDataFrame, 
                    stats_cols: list) -> gpd.GeoDataFrame:
    df = df.assign(
        # route_length is in feet
        route_length = df.geometry.to_crs(geography_utils.CA_StatePlane).length
    )
    # Calculate per mile stats
    for c in stats_cols:
        df[f"{c}_per_mi"] = round(
            df[c].divide(df.route_length) * geography_utils.FEET_PER_MI, 2)

    return df

        
if __name__=="__main__":

    # (1) Highway segments, draw buffer of 50 ft
    highways = catalog.segmented_highways.read()
    highways_polygon = draw_buffer(highways, buffer=50)
    
    # (2) Spatial join to keep highway segments that have buses running on SHN
    highway_segments_with_bus = sjoin_bus_routes_to_hwy_segments(highways_polygon)
        
    # (3) Spatial join stops to highway segments, also attach stop_times for those stops
    stop_times_on_hwy = sjoin_stops_to_highway_segments_get_stop_times(highways_polygon)
        
    # (4a) Aggregate stops and stop_times to segments 
    by_route_segment = aggregated_route_stats.compile_peak_all_day_aggregated_stats(
        stop_times_on_hwy, route_segment_cols, 
        stat_cols = {"departure_hour": "count", 
                     "stop_id": "nunique"})
    
    # (4b) Merge in number of trips for that route to segments
    # Trips are different than stops and stop_times, because we
    # want all the trips passing through to count
    # not just count the ones where the physical stop is present in segment
    trips_by_route = (catalog.bus_routes_aggregated_stats.read()
                      [route_cols + ["trips_all_day", "trips_peak"]]
                     )
    
    by_route_segment2 = pd.merge(
        by_route_segment,
        trips_by_route,
        on = route_cols,
        how = "left",
        # many on left because a route_id can be sjoined to multiple hwy_segment_ids
        validate = "m:1"
    )
    
    segments_stats_with_geom = build_highway_segment_table(
        by_route_segment2, highways, highways_polygon)
    
    utils.geoparquet_gcs_export(
        segments_stats_with_geom.to_crs(geography_utils.WGS84), 
        GCS_FILE_PATH, 
        "highway_segment_stats")