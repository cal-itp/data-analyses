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
        

def spatial_join_stops_to_highway_segments(
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


def filter_stop_times_to_stops_on_hwy(stops_on_hwy: gpd.GeoDataFrame) -> dd.DataFrame:
    """
    Filter stop times table down to the stops that have been attached
    to hwy segments.
    
    Turn into dask dataframe by the end.
    """
    stop_times_with_hr = catalog.stop_times_for_routes_on_shn.read()
    
    stop_times_on_hwy = pd.merge(
        stop_times_with_hr, 
        stops_on_hwy[["calitp_itp_id", "stop_id", "hwy_segment_id"]],
        on = ["calitp_itp_id", "stop_id"],
        how = "inner"
    )
    
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


if __name__=="__main__":

    # (1) Highway segments, draw buffer of 50 ft
    highways = catalog.segmented_highways.read()
    highways_polygon = draw_buffer(highways, buffer=50)
    
    # (2) Spatial join to keep highway segments that have buses running on SHN
    highway_segments_with_bus = sjoin_bus_routes_to_hwy_segments(highways_polygon)
    
    # (3) Spatial join stops to highway segments
    stops_on_hwy = spatial_join_stops_to_highway_segments(highways_polygon)
    
    # (4a) Filter the stop_times table down to stops attached to hwy segments
    stop_times_on_hwy = filter_stop_times_to_stops_on_hwy(stops_on_hwy)
    
    # (4b) Aggregate stops and stop_times to segments 
    by_route_segment = aggregated_route_stats.compile_peak_all_day_aggregated_stats(
        stop_times_on_hwy, route_segment_cols, 
        stat_cols = {"trip_id": "nunique", 
                     "departure_hour": "count", 
                     "stop_id": "nunique"})
    
    
    # (5a) Now aggregate to hwy_segment_id, since there can be multiple routes 
    # on same segment_id
    segments_with_geom = aggregate_to_hwy_segment(
        by_route_segment, highways, ["hwy_segment_id"])
    
    # (5b) Add in average speed, aggregated to hwy_segment_id
    weighted_speeds = calculate_trip_weighted_speed(
        stops_on_hwy, ["hwy_segment_id"])
    
    segments_with_geom_and_speed = pd.merge(
        segments_with_geom, 
        weighted_speeds, 
        on = "hwy_segment_id",
        how = "inner",
    )
    
    utils.geoparquet_gcs_export(segments_with_geom_and_speed, 
                                GCS_FILE_PATH, 
                                "highway_segment_stats")