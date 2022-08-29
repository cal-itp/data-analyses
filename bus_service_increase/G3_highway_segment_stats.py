"""
Compile route-level data for mapping / viz?
"""
import intake
import geopandas as gpd
import pandas as pd

from shared_utils import geography_utils, utils
from utils import GCS_FILE_PATH

catalog = intake.open_catalog("*.yml")

highway_cols = ["District", "County", "hwy_segment_id"]
route_cols = ["calitp_itp_id", "route_id"]


def sjoin_route_stats_to_highway_segments(
    highways: gpd.GeoDataFrame, bus_routes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Highways segments are for all CA.
    Bus routes contains the routes where at least 50% of route runs on SHN.
    """
    # Spatial join to find the highway segments with buses that run on SHN    
    s1 = gpd.sjoin(
        highways[highway_cols + ["geometry"]].drop_duplicates(),
        bus_routes[route_cols + ["geometry"]].drop_duplicates(),
        how = "inner",
        predicate="intersects"
    ).drop(columns = "index_right")
    
    return s1 


def aggregate_to_hwy_segments(highway_segments: gpd.GeoDataFrame, 
                              route_stats: pd.DataFrame) -> gpd.GeoDataFrame:
    
    segments_with_route_stats = pd.merge(
        highway_segments,
        route_stats,
        on = route_cols,
        how = "inner",
        validate = "m:1"
    )
    
    sum_cols = [
        "trips_peak", "trips_all_day", 
        "stop_arrivals_peak", "stop_arrivals_all_day",
        "stops_peak", "stops_all_day"
    ]
    
    segment_stats = geography_utils.aggregate_by_geography(
        segments_with_route_stats,
        group_cols = highway_cols + ["service_date"],
        sum_cols = sum_cols,
        nunique_cols = route_cols,
    ).rename(columns = {"calitp_itp_id": "num_itp_id", 
                        "route_id": "num_route_id"})

    segment_stats2 = geography_utils.attach_geometry(
        segment_stats,
        segments_with_route_stats[highway_cols + ["geometry"]].drop_duplicates(),
        merge_col = highway_cols,
        join="inner"
    )
    
    return segment_stats2


if __name__=="__main__":

    # (1) Spatial join to keep highway segments that have buses running on SHN
    bus_routes = catalog.bus_routes_on_hwys.read().rename(
        columns = {"itp_id": "calitp_itp_id"})

    highways = catalog.segmented_highways.read()
    
    highways_with_buses = sjoin_route_stats_to_highway_segments(
        highways, bus_routes)
    
    # (2) Just use stats at route-level for stops that actually fall on SHN
    stats_on_hwy = catalog.bus_stops_aggregated_stats.read()
    
    highway_stats = aggregate_to_hwy_segments(s
        highways_with_buses, stats_on_hwy)
    
    utils.geoparquet_gcs_export(highway_stats, 
                                GCS_FILE_PATH, 
                                "highway_segment_stats")