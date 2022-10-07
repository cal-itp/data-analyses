import geopandas as gpd

from shared_utils import rt_dates, rt_utils, geography_utils

SELECTED_DATE = rt_dates.DATES["sep2022"]
COMPILED_CACHED_VIEWS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"
GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/regional_bus_network/'


def aggregate_to_route(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Aggregate trip-level df to route-level.
    Route-level does still have multiple origin/destination pairs.
    """
    route_cols = ["calitp_itp_id", "route_id", 
                  "route_type", 
                  "origin_stop_name", "destination_stop_name"
                 ]
    
    num_trips_by_route = geography_utils.aggregate_by_geography(
        gdf,
        group_cols = route_cols,
        nunique_cols = ["trip_id"],
        rename_cols = True
    )
    
    route_df = geography_utils.attach_geometry(
        num_trips_by_route,
        gdf[route_cols + ["origin", "destination", "geometry"]].drop_duplicates(),
        merge_col = route_cols,
        join = "inner"
    )
    
    return route_df