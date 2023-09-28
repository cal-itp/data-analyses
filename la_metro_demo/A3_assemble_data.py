"""
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import geography_utils
from shared_utils import portfolio_utils, rt_dates
from bus_service_utils import calenviroscreen_lehd_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, COMPILED_CACHED_VIEWS

import A2_clean_up_gtfs as clean_up_gtfs

PROJECT_CRS = geography_utils.CA_NAD83Albers
BUS_SERVICE_GCS = "gs://calitp-analytics-data/data-analyses/bus_service_increase/"

    
def import_calenviroscreen_tracts():
    """
    Import CalEnviroScreen tracts and separate percentiles into 3 groups. 
    """
    tracts = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}calenviroscreen_lehd_by_tract.parquet",
        columns = ["Tract", "overall_ptile", "geometry"]
    ).to_crs(PROJECT_CRS)

    calenviroscreen_tracts = calenviroscreen_lehd_utils.define_equity_groups(
        tracts,
        percentile_col = ["overall_ptile"],
        num_groups = 3
    )
    
    return calenviroscreen_tracts


def aggregate_overlay_intersect_by_equity(
    gdf: gpd.GeoDataFrame) -> pd.DataFrame: 
    """
    Aggregate by route-equity_group and get the distribution of 
    route intersecting with the 3 equity groups.
    Also count the number of tracts the route is passing through
    for each equity group.
    """
    gdf = gdf.assign(
        intersect_length = gdf.geometry.length,
        overall_ptile_group = gdf.overall_ptile_group.fillna(0),
    )
    
    route_cols = ["name", "route_id", "route_short_name", 
                  "route_length"]
    equity_cols = ["overall_ptile_group"]
    
    by_route_equity = portfolio_utils.aggregate_by_geography(
        gdf,
        group_cols = route_cols + equity_cols,
        sum_cols = ["intersect_length"],
        nunique_cols = ["Tract"]
    )
    
    return by_route_equity
    
    
def pct_route_in_equity_community(
    gdf: gpd.GeoDataFrame
) -> pd.DataFrame:
    """
    Calculate how much of each route intersects with tracts of equity_group==3.
    This captures how much of each route runs through equity communities.
    """
    gdf = gdf.assign(
        percent_in_equity = gdf.intersect_length.divide(gdf.route_length)
    )
    
    in_equity_community = (gdf[gdf.overall_ptile_group==3]
                           .drop(columns = ["overall_ptile_group", 
                                            "intersect_length"])
                           .reset_index(drop=True)
                          )
    
    return in_equity_community


def get_processed_route_data():
    """
    """
    routes_with_geom = (clean_up_gtfs.assemble_route_level_data()
                        .drop(columns = "_merge")
                       )
    
    calenviroscreen_tracts = import_calenviroscreen_tracts()
    
    route_to_tracts = gpd.overlay(
        routes_with_geom,
        calenviroscreen_tracts,
        how = "intersection",
    )
    
    by_equity_groups = aggregate_overlay_intersect_by_equity(route_to_tracts)
    route_in_equity = pct_route_in_equity_community(by_equity_groups)
    
    routes_with_info = pd.merge(
        routes_with_geom,
        route_in_equity,
        on = ["name", "route_id", "route_short_name", "route_length"],
        how = "left",
        validate = "1:1",
    )
    
    return routes_with_info