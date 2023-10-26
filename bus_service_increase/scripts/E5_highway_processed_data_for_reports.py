import geopandas as gpd
import intake
import pandas as pd

import E1_get_buses_on_shn as get_buses_on_shn
import E4_highway_segments_stats as highway_utils
import D5_make_stripplot_data as D5

from E0_bus_oppor_vars import ANALYSIS_DATE, GCS_FILE_PATH
from parallel_corridors_utils import PCT_COMPETITIVE_THRESHOLD
from calitp_data_analysis import geography_utils, utils
from shared_utils import portfolio_utils

catalog = intake.open_catalog("./*.yml")

#--------------------------------------------------------------------#
# Transit Deserts
#--------------------------------------------------------------------#
def find_hwy_segments_no_buses(segment_distance: int) -> gpd.GeoDataFrame: 
    # Use bus routes with 1.5x competitive cutoff
    bus_routes = catalog.bus_routes_all_aggregated_stats.read().to_crs(
        geography_utils.CA_StatePlane)

    highway_segments = get_buses_on_shn.cut_highway_segments(segment_distance)

    # Get a 2 mile buffer around highway 10 mi segments
    highway_polygon = highway_utils.draw_buffer(
        highway_segments, 
        geography_utils.FEET_PER_MI * 2)

    # Find which highway segments have buses
    hwys_with_buses = highway_utils.sjoin_bus_routes_to_hwy_segments(
        highway_polygon, bus_routes
    )
    
    # Now, get the converse of that. Find highways without buses.
    # Do a merge this way and keep left_only to be safe
    highway_segments2 = pd.merge(
        highway_segments,
        hwys_with_buses[["hwy_segment_id"]].drop_duplicates(),
        how = "left",
        validate = "1:1",
        indicator=True
    )
    
    no_buses = (highway_segments2[highway_segments2._merge=="left_only"]
                .drop(columns = "_merge"))

    no_buses2 = remove_too_short_segments(no_buses, segment_distance)
                                                                           
    return no_buses2


def remove_too_short_segments(gdf: gpd.GeoDataFrame, 
                              segment_distance: int) -> gpd.GeoDataFrame: 
    """
    Remove highway corridor segments that are too short.
    If Route, RouteType change in the middle of a highway
    the corridor could get cut to a length shorter than the desired.
    
    Too short means if it's less than 75% of the desired segment_length.
    For a 10 mi highway corridor, segment needs to be 7.5 mi long.
    """
    gdf = gdf.to_crs(geography_utils.CA_StatePlane)
    
    gdf = gdf.assign(
        length = gdf.geometry.length
    )
    
    gdf2 = (gdf[gdf.length >= segment_distance * 0.75]
                 .drop(columns = ["length"])
                 .reset_index(drop=True)
                )
    
    return gdf2


#--------------------------------------------------------------------#
# Uncompetitive transit routes
#--------------------------------------------------------------------#
def find_uncompetitive_competitive_on_hwys(
    analysis_date: str, segment_distance: int) -> gpd.GeoDataFrame: 
    # Grab bus routes that don't meet threshold of 2x competitive cutoff
    bus_routes = D5.assemble_data(analysis_date, threshold = 2)
    
    # D5 returns trip-level data...get this to route-level
    bus_routes = bus_routes[
        ["calitp_itp_id", "route_id", 
         "num_trips", "num_competitive", 
         "pct_trips_competitive", "geometry"]].drop_duplicates()

    # These routes are considered uncompetitive, becasue less than half the trips they
    # run are competitive to car
    bus_routes = bus_routes.assign(
        uncompetitive = bus_routes.apply(
            lambda x:
            1 if x.pct_trips_competitive <= PCT_COMPETITIVE_THRESHOLD 
            else 0, axis = 1)
    )
    
    highway_segments = get_buses_on_shn.cut_highway_segments(segment_distance)

    # Get a 2 mile buffer around highway 10 mi segments
    highway_polygon = highway_utils.draw_buffer(
        highway_segments, 
        geography_utils.FEET_PER_MI * 2)
    
    # Find which highway segments have bus routes 
    # with uncompetitive/competitive flag merged in
    hwys_with_buses = highway_utils.sjoin_bus_routes_to_hwy_segments(
        highway_polygon, bus_routes
    ).merge(
        bus_routes[["calitp_itp_id", "route_id", "uncompetitive"]],
        on = ["calitp_itp_id", "route_id"],
        how = "inner",
        validate = "m:1"
    )
    
    uncompetitive_hwy_segments = get_pct_uncompetitive_by_highway_segment_id(
        hwys_with_buses)
    
    # Merge all the highway segment geom back in
    highway_segments2 = pd.merge(
        highway_segments,
        uncompetitive_hwy_segments[
            ["hwy_segment_id", "pct_uncompetitive"]].drop_duplicates(),
        how = "left",
        validate = "1:1",
        indicator=True
    )

    highway_segments2 = highway_segments2.assign(
        competitive_category = highway_segments2.apply(
            lambda x: tag_categories(x), axis=1)
    )
    
    return highway_segments2


def get_pct_uncompetitive_by_highway_segment_id(
    gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame: 
    """
    Get only the highway segments where all of the transit that
    does intersect with that segment are uncompetitive.
    """
    a1 = portfolio_utils.aggregate_by_geography(
        gdf,
        group_cols = ["hwy_segment_id"],
        count_cols = ["uncompetitive"],
        sum_cols = ["uncompetitive"],
        rename_cols = True
    ).rename(columns = {"uncompetitive_count": "num_routes"})
    
    a1 = a1.assign(
        pct_uncompetitive = a1.uncompetitive_sum.divide(a1.num_routes).round(2)
    )
        
    return a1


def tag_categories(row): 
    # Group highway segments into 4 categories - no transit at all
    # both uncompetitive/competitive transit, or just uncompetitive transnit
    # and uncategorized (nothing should be here...use as sanity check)
    if row._merge=="left_only":
        return "no transit"
    elif (row._merge=="both") and (row.pct_uncompetitive == 1):
        return "uncompetitive transit only"
    elif ((row._merge=="both") and (row.pct_uncompetitive < 1)):
        return "competitive and uncompetitive transit" 
    else:
          return "uncategorized"
        
        
if __name__ == "__main__":
    # Draw highway segments as 10 mile corridors
    SEGMENT_DISTANCE = geography_utils.FEET_PER_MI * 10

    # Get highway segments with no buses within 2 miles 
    hwys_no_buses = find_hwy_segments_no_buses(SEGMENT_DISTANCE)
    utils.geoparquet_gcs_export(
        hwys_no_buses, 
        GCS_FILE_PATH,
        f"highway_segments_no_transit_{ANALYSIS_DATE}"
    )
    
    # Get highway segments categorized into uncompetitive / no transit 
    # / both uncompetitive & competitive
    hwys_by_competitive_category = find_uncompetitive_competitive_on_hwys(
        ANALYSIS_DATE, SEGMENT_DISTANCE)
    
    utils.geoparquet_gcs_export(
        hwys_by_competitive_category,
        GCS_FILE_PATH,
        f"highway_segments_by_competitive_category_{ANALYSIS_DATE}"
    )