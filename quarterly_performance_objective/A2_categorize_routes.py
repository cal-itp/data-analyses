"""
Categorize routes created from A1_generate_routes_on_shn.py
and save geoparquet.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from A1_generate_routes_on_shn_data import merge_routelines_with_trips
from shared_utils import geography_utils, utils
from bus_service_utils import create_parallel_corridors
from update_vars import ANALYSIS_DATE, BUS_SERVICE_GCS, COMPILED_CACHED_GCS

logger.add("./logs/A2_categorize_routes.log")
logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

route_cols = ["itp_id", "route_id"]

#---------------------------------------------------------------#
# Data processing - merge trips dfs, tag a route as parallel/on shn/other
#---------------------------------------------------------------#

def calculate_route_level_service_hours(analysis_date: str) -> pd.DataFrame:
    """
    Merge trips df with trips_with_service_hrs (aggregated to shape_id).
    
    Aggregate this to route_level, since shape_id is confusing
    and leads to double-counting, or merges not going through in full.
    """
    trips_with_hrs = pd.read_parquet(
        f"{BUS_SERVICE_GCS}trips_with_hrs_{analysis_date}.parquet").rename(
        columns = {"calitp_itp_id": "itp_id"})
    
    trips = pd.read_parquet(
        f"{COMPILED_CACHED_GCS}trips_{analysis_date}_all.parquet").rename(
        columns = {"calitp_itp_id": "itp_id"})
    
    # Aggregate trips with service hours (at shape_id level) up to route_id
    route_service_hours = geography_utils.aggregate_by_geography(
        trips_with_hrs,
        group_cols = route_cols,
        sum_cols = ["total_service_hours"]
    ) 
    
    # Aggregate trips (at trip_id level) to route_id
    routes = trips[route_cols].drop_duplicates().reset_index(drop=True)
    
    # there are multiple trips sharing same shape_id
    # that's fine, but since trips_with_hrs is already aggregated up to
    # the route_id level, aggregate for trips too
    route_full_info = pd.merge(
        routes,
        route_service_hours,
        on = route_cols,
        how = "outer",
        validate = "1:1",
        indicator=True
    ).rename(columns = {"calitp_itp_id": "itp_id"})

    
    return route_full_info

    
def get_on_shn_routes(date_str: str) -> pd.DataFrame:
     # If it is on shn, we want to flag as 1
    df = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_on_shn_{date_str}.parquet")
    
    df2 = df[df.parallel==1]
    
    df3 = get_unique_routes(df2)
    
    return df3


def get_intersecting_routes(date_str: str) -> pd.DataFrame:
    # These are routes that are affected by SHN
    # Since the requirements here are less stringent than on_shn
    # So, remove those that are already tagged as on_shn
    df = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}parallel_or_intersecting_{date_str}.parquet"
    )
    df2 = df[df.parallel == 1]

    df3 = get_unique_routes(df2)
    
    return df3


def get_unique_routes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get it down to unique route for each row. 
    As is, a row is route_id-hwy, the segment of overlap
    """
    # If there are multiple shape_ids for route_id,
    # Keep the one where it's has higher overlap with SHN
    # If it was ever tagged as parallel, let's keep that obs    
    df2 = (df.sort_values(route_cols + ["pct_route", "pct_highway"],
                          ascending=[True, True, False, False]
                         )
           .drop_duplicates(subset=route_cols)
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
    

def add_district(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    Merge in district info (only 1 district per route_id)
    """
    
    on_shn = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_on_shn_{date_str}.parquet")
    
    district = on_shn[route_cols + ["pct_route", "District"]].drop_duplicates()
    
    # If there's multiple districts, keep the one associated with the highest % route 
    # since that's the obs we keep anyway
    district2 = (district.sort_values(route_cols + ["pct_route", "District"], 
                                     ascending=[True, True, False, True])
                 .drop_duplicates(subset=route_cols)
                 .reset_index(drop=True)
                 [route_cols + ["District"]]
    )
    
    df2 = pd.merge(
        df, district2,
        on = route_cols,
        how = "left",
        validate = "1:1"
    ).astype({"District": "Int64"})
    
    return df2

    
def flag_shn_intersecting_routes(analysis_date: str) -> pd.DataFrame:
    """
    Take the trips df (each indiv trip) and aggregated trip_service_hrs df
    (aggregated to shape_id), merge together,
    and flag whether a transit route is on SHN, intersects SHN, or other.
    """
    # Merge trips and trips_with_hrs dfs, and aggregate to route_level
    route_level_df = calculate_route_level_service_hours(analysis_date)
    
    # Flag routes if they're parallel or on SHN
    on_shn_routes = get_on_shn_routes(analysis_date)
    intersecting_routes = get_intersecting_routes(analysis_date)
    
    # Merge the parallel and on_shn dummy variables in
    with_on_shn_flag = pd.merge(route_level_df, 
                                on_shn_routes,
                                on = route_cols,
                                how = "left",
                                validate = "1:1",
                                indicator="in_on_shn"
    )
    
    with_intersecting_flag = pd.merge(with_on_shn_flag,
                                      intersecting_routes,
                                      on = route_cols,
                                      how = "left", 
                                      validate = "1:1",
                                      indicator="in_intersecting"
    )
    
    # Make sure on_shn, intersects_shn, and other are mutually exclusive categories
    # A route can only fall into 1 of these groups
    with_categories = mutually_exclusive_groups(with_intersecting_flag)
    with_categories = add_district(with_categories, analysis_date)
    
    return with_categories


if __name__=="__main__":
    
    start = datetime.datetime.now()
    
    # (1) Categorize into each group
    df = flag_shn_intersecting_routes(ANALYSIS_DATE)
    
    time1 = datetime.datetime.now()
    logger.info(f"flag shn or intersecting: {time1 - start}")
    
    # (2) Add in the route's line geom 
    trips_with_geom = merge_routelines_with_trips(ANALYSIS_DATE)
    
    bus_route_with_geom = create_parallel_corridors.process_transit_routes(
        alternate_df = trips_with_geom)
    
    gdf = pd.merge(
        bus_route_with_geom[["itp_id", "route_id", "route_length", "geometry"]], 
        df,
        on = ["itp_id", "route_id"],
        how = "left",
    ).to_crs(geography_utils.CA_StatePlane)  # make sure units are in feet
    
    time2 = datetime.datetime.now()
    logger.info(f"merge in line geom: {time2 - time1}")
    
    # (3) Some cleanup for visualization, as well as simplifying geom for faster mapping
    gdf = gdf.assign(
        total_service_hours = gdf.total_service_hours.round(2),
        # simplify geom to make it faster to plot?
        geometry = gdf.geometry.simplify(tolerance = 20),
        route_length_mi = round(gdf.route_length / geography_utils.FEET_PER_MI, 2),
    ).drop(columns = ["route_length"]).to_crs(geography_utils.WGS84)
    
    # Export to GCS (use date suffix because we will want historical comparisons)
    utils.geoparquet_gcs_export(
        gdf, 
        BUS_SERVICE_GCS,
        f"routes_categorized_{ANALYSIS_DATE}"
    )
    
    logger.info("exported dataset to GCS")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")