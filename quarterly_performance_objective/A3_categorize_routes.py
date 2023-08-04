"""
Categorize routes created from A1_generate_routes_on_shn.py
and save geoparquet.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from shared_utils import geography_utils, utils
from update_vars import (BUS_SERVICE_GCS, 
                         ANALYSIS_DATE, VERSION)

def import_data(
    analysis_date: str, 
) -> tuple[gpd.GeoDataFrame]:
    """
    Import the data needed.
    """
    transit_routes = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_{analysis_date}.parquet") 

    on_shn_routes = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_on_shn_{analysis_date}.parquet")
    
    intersecting_routes = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}parallel_or_intersecting_{analysis_date}.parquet")
    
    return transit_routes, on_shn_routes, intersecting_routes
    

#---------------------------------------------------------------#
# Data processing - merge trips dfs, tag a route as parallel/on shn/other
#---------------------------------------------------------------#
def get_unique_routes(df: pd.DataFrame, route_cols: list) -> pd.DataFrame:
    """
    route_cols: list that uniquely identifies an operator-route_id.
    Get it down to unique route for each row. 
    As is, a row is route_id-hwy, the segment of overlap
    """
    # If there are multiple shape_ids for route_id,
    # Keep the one where it's has higher overlap with SHN
    # If it was ever tagged as parallel, let's keep that obs    
    df2 = (df.sort_values(route_cols + ["pct_route", "pct_highway"])
           .drop_duplicates(subset=route_cols, keep="last")
           # keep last because route_cols can 
           # be 2 cols (feed_key, name) or 1 col (calitp_itp_id) and can't control
           # sort order without knowing list length
          )[route_cols]
    
    return df2
    

def mutually_exclusive_groups(df: pd.DataFrame) -> pd.DataFrame:
    # Now, force mutual exclusivity
    def make_mutually_exclusive(row: str) -> str:
        if row.in_on_shn=="both":
            return "on_shn"
        elif (row.in_on_shn=="left_only") and (row.in_intersecting=="both"):
            return "intersects_shn"
        elif (row.in_on_shn=="left_only") and (row.in_intersecting=="left_only"):
            return "other"
    
    df2 = df.assign(
        category = df.apply(lambda x: make_mutually_exclusive(x), axis=1),
        # Flag a unique route, since nunique(route_id) isn't exact, if route_id is 1
        # and many operators share that value
        unique_route = 1
    ).drop(columns = ["in_on_shn", "in_intersecting"])
    
    return df2
    
    
def flag_shn_intersecting_routes(
    all_routes: gpd.GeoDataFrame,
    on_shn_routes: pd.DataFrame,
    intersecting_routes: pd.DataFrame,
    route_cols: list
) -> pd.DataFrame:
    """
    Take the trips df (each indiv trip) and aggregated trip_service_hrs df
    (aggregated to shape_id), merge together,
    and flag whether a transit route is on SHN, intersects SHN, or other.
    """

    # Merge dummy variables in
    with_on_shn_intersecting = pd.merge(
        all_routes,
        on_shn_routes,
        on = route_cols,
        how = "left",
        validate = "1:1",
        indicator = "in_on_shn"
    ).merge(
        intersecting_routes,
        on = route_cols,
        how = "left",
        validate = "1:1",
        indicator = "in_intersecting"
    )

    # Make sure on_shn, intersects_shn, and other are mutually exclusive categories
    # A route can only fall into 1 of these groups
    with_categories = mutually_exclusive_groups(with_on_shn_intersecting)
    
    return with_categories


if __name__=="__main__":

    logger.add("./logs/A3_categorize_routes.log", retention = "6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}  warehouse {VERSION}")
    start = datetime.datetime.now()

    route_cols = ["feed_key", "name", "route_id"]
    
    # (1) Get unique transit routes 
    transit_routes, on_shn_routes, intersecting_routes = import_data(
        ANALYSIS_DATE)
    
    on_shn = get_unique_routes(
        on_shn_routes[on_shn_routes.parallel==1],
        route_cols
    )
    
    intersecting = get_unique_routes(
        intersecting_routes[intersecting_routes.parallel==1],
        route_cols
    )
    
    # (2) Categorize into each group
    df = flag_shn_intersecting_routes(
        transit_routes,
        on_shn,
        intersecting,
        route_cols
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"flag shn or intersecting: {time1 - start}")
    
    
    time2 = datetime.datetime.now()
    
    # (3) Some cleanup for visualization
    gdf = (df.assign(
        service_hours = df.service_hours.round(2),
        route_length_mi = round(df.route_length / geography_utils.FEET_PER_MI, 2),
        ).drop(columns = ["route_length"])
        .to_crs(geography_utils.WGS84)
    )
    
    # Export to GCS (use date suffix because we will want historical comparisons)    
    utils.geoparquet_gcs_export(
        gdf, 
        BUS_SERVICE_GCS,
        f"routes_categorized_{ANALYSIS_DATE}"
    )
    
    logger.info("exported dataset to GCS")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")