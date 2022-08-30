"""
Compile route-level data for mapping / viz?
"""
import dask.dataframe as dd
import intake
import geopandas as gpd
import pandas as pd

import G2_aggregated_stats
from shared_utils import geography_utils, utils
from utils import GCS_FILE_PATH
from G1_get_buses_on_shn import ANALYSIS_DATE, TRAFFIC_OPS_GCS

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


def spatial_join_stops_to_highway_segments(
    highways: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Spatial join highway segments polygon to stops.
    """
    stops = gpd.read_parquet(f"{TRAFFIC_OPS_GCS}stops_{ANALYSIS_DATE}.parquet")
    
    stops_on_hwys = gpd.sjoin(
        stops.to_crs(geography_utils.CA_StatePlane), 
        highways.to_crs(geography_utils.CA_StatePlane)[["hwy_segment_id", "geometry"]],
        how = "inner",
        predicate="intersects"
    ).drop(columns = "index_right")

    # Only keep sjoin if it's the same calitp_itp_id
    stops_on_hwys2 = (stops_on_hwys.drop_duplicates(
        subset=["calitp_itp_id", "stop_id", "hwy_segment_id"]
        ).reset_index(drop=True)
    )
    
    return stops_on_hwys2


def aggregate_to_hwy_segment(gdf: gpd.GeoDataFrame, 
                             group_cols: list) -> gpd.GeoDataFrame:
    
    sum_cols = [
        'trips_peak', 'trips_all_day', 
        'stop_arrivals_peak', 'stop_arrivals_all_day',
        'stops_peak', 'stops_all_day'
    ]
    
    segment = geography_utils.aggregate_by_geography(
        gdf,
        group_cols = group_cols,
        sum_cols = sum_cols,
    )
    
    segment_with_geom = geography_utils.attach_geometry(
        segment,
        gdf[group_cols + ["geometry"]].drop_duplicates(),
        merge_col = group_cols,
        join = "inner"
    )
    
    segment_with_geom[sum_cols] = segment_with_geom[sum_cols].astype(int)
    segment_with_geom = segment_with_geom.reindex(columns = group_cols + 
                                                  sum_cols + ["geometry"])
    
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
    stop_times_with_hr = catalog.stop_times_for_routes_on_shn.read()
    
    stop_times_on_hwy = pd.merge(
        stop_times_with_hr, 
        stops_on_hwy[["calitp_itp_id", "stop_id", "hwy_segment_id"]],
        on = ["calitp_itp_id", "stop_id"],
        how = "inner"
    )
    
    stop_times_on_hwy = dd.from_pandas(stop_times_on_hwy, npartitions=1)
    
    # (4b) Aggregate stops and stop_times to segments 
    by_route_segment = G2_aggregated_stats.compile_peak_all_day_aggregated_stats(
        stop_times_on_hwy, route_segment_cols, 
        stat_cols = {"departure_hour": "count", 
                     "stop_id": "nunique"})
    
    # (5) Merge in the trip, stop_times, and stops aggregated stats
    # (5a) Merge in trip stats at route-level 
    route_stats = catalog.bus_routes_aggregated_stats.read()
    
    keep_cols = route_cols + ["service_date", "trips_peak", "trips_all_day"]
    
    segments_with_trips = pd.merge(
        highway_segments_with_bus, 
        route_stats[keep_cols],
        on = route_cols,
        how = "inner",
    )
    
    # (5b) Merge in stop_times and stops by segment
    segments_with_stops = pd.merge(
        segments_with_trips,
        by_route_segment,
        on = route_segment_cols,
        how = "left"
    )
    
    # (5c) Now aggregate to hwy_segment_id, since there can be multiple routes 
    # on same segment_id
    segments_with_geom = aggregate_to_hwy_segment(segments_with_stops, highway_cols)

    utils.geoparquet_gcs_export(segments_with_geom, 
                                GCS_FILE_PATH, 
                                "highway_segment_stats")