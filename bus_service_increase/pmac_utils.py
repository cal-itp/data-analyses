"""
Functions to wrangle PMAC datasets created from D1_pmac_routes.py
"""
import geopandas as gpd
import pandas as pd

from utils import GCS_FILE_PATH
from shared_utils import geography_utils

def merge_trips_with_service_hrs(trips: pd.DataFrame, 
                                 trips_with_hrs: pd.DataFrame) -> pd.DataFrame:
    """
    Merge trips df with trips_with_service_hrs (aggregated to shape_id).
    
    Each row should be calitp_itp_id-route_id-shape_id level.
    """
    shape_id_cols = [
        "calitp_itp_id", "calitp_url_number", 
        "route_id", "shape_id"
    ]

    trips_full_info = pd.merge(
        # there are multiple trips sharing same shape_id
        # that's fine, but since trips_with_hrs is already aggregated up to
        # the shape_id level, aggregate for trips too
        trips[shape_id_cols].drop_duplicates(),
        trips_with_hrs,
        on = shape_id_cols, 
        how = "outer",
        validate = "1:1",
        indicator=True
    )
    
    # Don't keep url_number and make sure no duplicates are around
    trips_full_info2 = (trips_full_info
                        .drop(columns = "calitp_url_number")
                        .drop_duplicates()
                        .rename(columns = {"calitp_itp_id": "itp_id"})
                       )
    
    return trips_full_info2

    
def get_parallel_routes(date_str):
     # If it is parallel, we want to flag as 1
    parallel = gpd.read_parquet(
        f"{GCS_FILE_PATH}parallel_or_intersecting_{date_str}.parquet")
    
    parallel = parallel[parallel.parallel==1]
    parallel2 = get_unique_routes(parallel)

    return parallel2


def get_on_shn_routes(date_str):
    # These are routes that have some part on SHN
    # BUT, there is overlap between the parallel
    # Since the requirements here are less stringent than parallel
    # So, remove those that are already parallel
    on_shn = gpd.read_parquet(
        f"{GCS_FILE_PATH}routes_on_shn_{date_str}.parquet"
    )
    
    on_shn2 = get_unique_routes(on_shn)
    on_shn2 = on_shn2.assign(
        on_shn = 1,
    ).drop(columns = "parallel")
    
    return on_shn2


def get_unique_routes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get it down to unique route for each row. 
    As is, a row is route_id-hwy, the segment of overlap
    """
    # If there are multiple shape_ids for route_id,
    # Keep the one where it's has higher overlap with SHN
    df = (df.sort_values(["itp_id", "route_id", "pct_route", "shape_id"],
                         ascending=[True, True, False, True],
                        )
                .drop_duplicates(subset=["itp_id", "route_id"])
                [["itp_id", "route_id", "District", "parallel"]]
                .reset_index(drop=True)
               )
    return df
    
    
def mutually_exclusive_groups(df):
    # Now, force mutual exclusivity
    def make_mutually_exclusive(row):
        if row.parallel==1:
            return "parallel"
        elif row.on_shn==1:
            return "on_shn"
        else:
            return "other"
    
    df["category"] = df.apply(lambda x: make_mutually_exclusive(x), axis=1)
    
    df2 = df.assign(
        is_parallel = df.apply(lambda x: 
                               1 if x.category == "parallel" else 0, axis=1),
        is_on_shn = df.apply(lambda x: 
                             1 if x.category == "on_shn" else 0, axis=1),
        is_other = df.apply(lambda x: 
                            1 if x.category == "other" else 0, axis=1),
    ).drop(columns = ["parallel", "on_shn"])
    
    return df2 
    
    
def flag_parallel_intersecting_routes(trips: pd.DataFrame, 
                                      trips_with_hrs: pd.DataFrame, 
                                      date_str: str) -> pd.DataFrame:
    """
    Take the trips df (each indiv trip) and aggregated trip_service_hrs df
    (aggregated to shape_id), merge together,
    and flag whether a transit route is parallel, on SHN, or other.
    """
    # Merge trips and trips_with_hrs dfs
    df = merge_trips_with_service_hrs(trips, trips_with_hrs)
    
    route_level_hours = geography_utils.aggregate_by_geography(
        df, 
        group_cols = ["itp_id", "route_id", "_merge"],
        sum_cols = ["total_service_hours"]
    )
    
    # Flag routes if they're parallel or on SHN
    parallel_routes = get_parallel_routes(date_str)
    on_shn_routes = get_on_shn_routes(date_str)
    
    # Merge the parallel and on_shn dummy variables in
    with_parallel_flag = pd.merge(route_level_hours, 
                                  parallel_routes,
                                  on = ["itp_id", "route_id"],
                                  how = "left",
                                  validate = "m:1"
    )
    
    with_shn_flag = pd.merge(with_parallel_flag,
                             on_shn_routes,
                             on = ["itp_id", "route_id", "District"],
                             how = "left",
                             validate = "m:1"
    )
    
    # Make sure parallel, on_shn, and other are mutually exclusive categories
    # A route can only fall into 1 of these groups
    with_categories = mutually_exclusive_groups(with_shn_flag)
    
    # Flag a unique route, since nunique(route_id) isn't exact, if route_id is 1
    # and many operators share that value
    with_categories = with_categories.assign(unique_route=1)

    return with_categories