"""
Pull datasets needed for 100 Recs for Better Buses
the same way, across directories.

1. transit routes needing major, corridor improvements:
    select_transit_routes_corridor_improvements()

2. transit routes needing marginal, hot spot improvements:
    select_transit_routes_hotspot_improvements()

3. sorted transit routes on speed and % trips competitive.
   used when districts don't meet criteria, but we still want to 
   make some recommendation.
    get_sorted_transit_routes()

4. highway segments with transit, but slow speeds:
    select_highway_corridors

"""
import geopandas as gpd
import pandas as pd

from typing import Literal

from shared_utils import geography_utils
from bus_service_utils import calenviroscreen_lehd_utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/bus_service_increase/"

def subset_by_speed_and_trip(gdf: gpd.GeoDataFrame, 
                             speed_dict: dict = {"speed": 10}, 
                             trip_dict: dict = {"trips": 5}
) -> gpd.GeoDataFrame:
    """
    Specify a speed and trip cut-off using a dictionary 
        key: column name
        value: column value 
    
    Speeds LESS THAN OR EQUAL TO cut-off are selected.
    Trips GREATER THAN OR EQUAL TO cut-off are selected.   
    """
    speed_col, speed_threshold = list(speed_dict.items())[0]
    trip_col, trip_threshold = list(trip_dict.items())[0]
    
    subset = gdf[(gdf[speed_col] <= speed_threshold) & 
                 (gdf[trip_col] >= trip_threshold) 
                ].reset_index(drop=True)
    
    return subset
    

def select_transit_routes_corridor_improvements(
    speed_dict: dict = {"mean_speed_mph": 12},
    trip_dict: dict = {"pct_trips_competitive": 0.10},
) -> gpd.GeoDataFrame:
    """
    Select transit routes for corridor improvements.
    Specify a speed and trip cut-off using a dictionary 
        key: column name
        value: column value 
    
    Speeds LESS THAN OR EQUAL TO cut-off are selected.
    % trips competitive GREATER THAN OR EQUAL TO cut-off are selected.    
       
    If the district doesn't have data available, use `get_sorted_transit_routes()`.

    Used in one_hundred_recs/major-route-improvements.ipynb
    """
    gdf = (gpd.read_parquet(
        f"{GCS_FILE_PATH}bus_routes_aggregated_stats.parquet")
        .to_crs(geography_utils.WGS84))
    
    # Only keep routes that intersect SHN
    gdf = gdf[gdf.category=="intersects_shn"]
    
    gdf2 = subset_by_speed_and_trip(gdf, speed_dict, trip_dict)
        
    return gdf2


def select_transit_routes_hotspot_improvements(
    speed_dict: dict = {"mean_speed_mph": 12},
    trip_dict: dict = {"pct_trips_competitive": 0.50},
) -> gpd.GeoDataFrame:
    """
    Select transit routes for corridor improvements.
    Specify a speed and trip cut-off using a dictionary 
        key: column name
        value: column value 
    
    Speeds GREATER THAN OR EQUAL TO cut-off are selected.
    % trips competitive GREATER THAN OR EQUAL TO cut-off are selected. 
    
    If the district doesn't have data available, use `get_sorted_transit_routes()`.
    
    Used in one_hundred_recs/marginal-route-improvements.ipynb
    """
    gdf = (gpd.read_parquet(
        f"{GCS_FILE_PATH}bus_routes_aggregated_stats.parquet")
        .to_crs(geography_utils.WGS84))
    
    # Since this is the only case selecting speeds higher than a threshold
    # do subsetting here
    speed_col, speed_threshold = list(speed_dict.items())[0]
    trip_col, trip_threshold = list(trip_dict.items())[0]
    
    gdf2 = gdf[(gdf[speed_col] >= speed_threshold) & 
               (gdf[trip_col] >= trip_threshold) & 
               (gdf.category.isin(["intersects_shn", "other"]))
              ].reset_index(drop=True)
    
    return gdf2


def get_sorted_transit_routes(
    recommendation_category: Literal["corridor", "hotspot"] = "corridor"
) -> gpd.GeoDataFrame:
    """
    Some districts don't meet the criteria. 
    Simply return a sorted df.
    """
    gdf = (gpd.read_parquet(
        f"{GCS_FILE_PATH}bus_routes_aggregated_stats.parquet")
        .to_crs(geography_utils.WGS84))
    
    if recommendation_category == "corridor":
        include_me = ["intersects_shn"]
    elif recommendation_category == "hotspot": 
        include_me = ["intersects_shn", "other"]
    else:
        include_me = ["on_shn", "intersects_shn", "other"]
    
    # There are NaN for caltrans_districts, because those routes exist, 
    # but since we can't plot anything from them...drop now
    gdf2 = (gdf[(gdf.category.isin(include_me)) & 
                (gdf.caltrans_district.notna())]
            .sort_values(["caltrans_district", "mean_speed_mph", "pct_trips_competitive"],
                         ascending = [True, True, True])
            .reset_index(drop=True)
           )

    return gdf2


def select_highway_corridors(
    speed_dict: dict = {"mean_speed_mph_trip_weighted": 12}, 
    trip_dict: dict = {"trips_all_day_per_mi": 2}
) -> gpd.GeoDataFrame:
    """
    Select highway corridors for investment. 
    Specify a speed and trip cut-off using a dictionary 
        key: column name
        value: column value     
    
    Speeds LESS THAN OR EQUAL TO cut-off are selected.
    Trips GREATER THAN OR EQUAL TO cut-off are selected.
    
    Used in bus_service_increase/highways-existing-transit.ipynb
    """
    gdf = gpd.read_parquet(
        f"{utils.GCS_FILE_PATH}highway_segment_stats.parquet")
    
    gdf2 = subset_by_speed_and_trip(gdf, speed_dict, trip_dict)
    
    return gdf2


def get_quartiles_by_district(
    gdf: gpd.GeoDataFrame, district_col: str = "District", 
    quartile_cols: list = [], num_groups: int = 4
):
    """
    Add quartiles by district, given a list of columns of interest.
    Drop NaNs and zeroes from that column, then get quartile off of remaining values.
    """

    def subset_by_district(gdf: gpd.GeoDataFrame, 
                           district: str | int, 
                           stat_col: str) -> gpd.GeoDataFrame:
        # extra filtering to only keep if trips > 0
        gdf2 = gdf[(gdf[district_col] == district) & 
                   (gdf[stat_col] > 0) & 
                   (gdf[stat_col].notna())
                  ].reset_index(drop=True)

        return gdf2

    
    gdf_with_quartiles = gpd.GeoDataFrame()
    
    for i in sorted(gdf[district_col].unique()):
        for c in quartile_cols: 
            district_df = subset_by_district(gdf, district = i, stat_col = c)
            
            if len(district_df) > 0:
                quartiles = calenviroscreen_lehd_utils.define_equity_groups(
                    district_df, percentile_col = [c], num_groups = num_groups
                )

                gdf_with_quartiles = pd.concat(
                    [gdf_with_quartiles, quartiles], 
                    axis=0, ignore_index=True)
            else: 
                continue

    return gdf_with_quartiles


