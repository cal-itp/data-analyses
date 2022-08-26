"""
Create route-level files needed for 100 bus improvement recommendations.

Output: 
    bus_routes_on_hwys (subset of routes that run on highways / 
            hwys it overlaps with, and includes bus route line geometry)
    segmented_highways (1 mi segments)
"""
import geopandas as gpd
import intake
import pandas as pd
import zlib

import create_parallel_corridors
from shared_utils import geography_utils, rt_dates, utils
from utils import GCS_FILE_PATH

catalog = intake.open_catalog("./*.yml")
ANALYSIS_DATE = rt_dates.DATES["may2022"]


# From D1_pmac_routes.py, routes_on_shn_{ANALYSIS_DATE}.parquet was created
# Can adjust thresholds here using same file
def grab_bus_routes_running_on_highways(
    analysis_date: str, pct_route: float = 0.5, 
    pct_highway: float = 0.2) -> gpd.GeoDataFrame:
    """
    Increase threshold for how much route overlaps with SHN
    to identify ones actually running on SHN.
    """
    gdf = gpd.read_parquet(f"{GCS_FILE_PATH}routes_on_shn_{analysis_date}.parquet")
        
    gdf = gdf.assign(
        parallel = gdf.apply(lambda x: 
                             1 if ((x.pct_route > pct_route) and 
                                   (x.pct_highway > pct_highway)) 
                             else 0, axis=1),
    )    
    
    parallel = (gdf[gdf.parallel == 1]
                .reset_index(drop=True)
               )
    
    integrify_me = ["Route", "District", "NB", "SB", "EB", "WB"]
    parallel[integrify_me] = parallel[integrify_me].astype(int)
    
    return parallel 


def cut_highway_segments(segment_distance: int) -> gpd.GeoDataFrame:
    """
    Cut highways into 1 mi segments.
    
    Figure out a way to layer how many total trips are running 
    across multiple operators for all bus routes, to get a 
    level of service operating on the SHN.
    """
    hwy_group_cols = ["Route", "County", "District", "RouteType"]

    highways = create_parallel_corridors.prep_highway_directions_for_dissolve(
        group_cols = hwy_group_cols
    )

    highways = highways.assign(
        highway_length = highways.geometry.length,
        Route = highways.Route.astype(int),
        District = highways.District.astype(int),
    )

    hwy_segments = geography_utils.cut_segments(
        highways, group_cols = hwy_group_cols, 
        segment_distance = segment_distance
    )
    
    # Create segment_id
    # high_quality_transit_areas/corridor_utils.py
    hwy_segments2 = hwy_segments.assign(
        hwy_segment_id = hwy_segments.apply(
            lambda x: 
            # this checksum hash always gives same value if 
            # the same combination of strings are given
            zlib.crc32(
                (str(x.Route) + x.RouteType + 
                 x.County + str(x.District) + x.segment_sequence)
                .encode("utf-8"))
            , axis=1),
    )
    
    return hwy_segments2


if __name__=="__main__":
    
    # (1) Grab bus routes that actually run on the highway
    # a row is calitp_itp_id-route_id-hwy, geometry is transit route geom
    bus_routes = grab_bus_routes_running_on_highways(
        ANALYSIS_DATE, pct_route = 0.5, pct_highway = 0.2)
    
    utils.geoparquet_gcs_export(bus_routes, GCS_FILE_PATH, "bus_routes_on_hwys")
    
    # (2) Cut highway segments at 1 mi segment lengths
    # We are already in EPSG:2229 (CA State Plane - ft)
    SEGMENT_DISTANCE = geography_utils.FEET_PER_MI
    
    highways = cut_highway_segments(SEGMENT_DISTANCE)
    
    utils.geoparquet_gcs_export(highways, GCS_FILE_PATH, "segmented_highways")