import geopandas as gpd
import pandas as pd

from bus_service_utils import calenviroscreen_lehd_utils

def get_quartiles_by_district(
    gdf: gpd.GeoDataFrame, district_col: str = "District", 
    quartile_cols: list = [], num_groups: int = 4
):
    """
    Add quartiles by district, given a list of columns of interest.
    Drop NaNs and zeroes from that column, then get quartile off of remaining values.
    """

    def subset_by_district(gdf: gpd.GeoDataFrame, 
                           district: str | int, stat_col: str) -> gpd.GeoDataFrame:
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


# For E5_plot_hwy_segments
def select_highway_corridors_100recs(gdf: gpd.GeoDataFrame, 
                                     speed_dict: dict = 
                                     {"mean_speed_mph_trip_weighted": 12}, 
                                     trip_dict: dict = {"trips_all_day_per_mi": 2}
                                    ) -> gpd.GeoDataFrame:
    """
    Select highway corridors for investment. Specify a speed and trip cut-off.
    Speeds LESS THAN OR EQUAL TO cut-off are selected.
    Trips GREATER THAN OR EQUAL TO cut-off are selected.
    """
    
    speed_col, speed_threshold = list(speed_dict.items())[0]
    trip_col, trip_threshold = list(trip_dict.items())[0]
    
    subset = gdf[(gdf[speed_col] <= speed_threshold) & 
                 (gdf[trip_col] >= trip_threshold)
                ]
    
    return subset