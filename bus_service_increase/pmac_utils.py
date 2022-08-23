"""
Functions to wrangle PMAC datasets created from D1_pmac_routes.py
"""
import altair as alt
import geopandas as gpd
import pandas as pd

from D1_pmac_routes import TRAFFIC_OPS_GCS
from utils import GCS_FILE_PATH
from shared_utils import geography_utils

#---------------------------------------------------------------#
# Data processing - merge trips dfs, tag a route as parallel/on shn/other
#---------------------------------------------------------------#

def merge_trips_with_service_hrs(date_str) -> pd.DataFrame:
    """
    Merge trips df with trips_with_service_hrs (aggregated to shape_id).
    
    Each row should be calitp_itp_id-route_id-shape_id level.
    """
    trips_with_hrs = pd.read_parquet(
        f"{GCS_FILE_PATH}trips_with_hrs_{date_str}.parquet")
    
    trips = pd.read_parquet(
        f"{TRAFFIC_OPS_GCS}trips_{date_str}.parquet")
    
    route_cols = [
        "calitp_itp_id", "route_id"
    ]
    
    route_service_hours = geography_utils.aggregate_by_geography(
        trips_with_hrs,
        group_cols = route_cols,
        sum_cols = ["total_service_hours"]
    ) 
    
    # there are multiple trips sharing same shape_id
    # that's fine, but since trips_with_hrs is already aggregated up to
    # the route_id level, aggregate for trips too
    route_full_info = pd.merge(
        trips[route_cols].drop_duplicates(),
        route_service_hours,
        on = route_cols,
        how = "outer",
        validate = "1:1",
        indicator=True
    ).rename(columns = {"calitp_itp_id": "itp_id"})

    
    return route_full_info

    
def get_parallel_routes(date_str: str) -> pd.DataFrame:
     # If it is parallel, we want to flag as 1
    parallel = gpd.read_parquet(
        f"{GCS_FILE_PATH}parallel_or_intersecting_{date_str}.parquet")
    
    parallel = parallel[parallel.parallel==1]
    
    # Grab district
    route_cols = ["itp_id", "route_id"]
    district_for_route = (parallel[parallel.District.notna()]
                          [route_cols + ["District"]]
                          .drop_duplicates(subset=route_cols)
    )
    
    parallel2 = get_unique_routes(parallel)

    parallel3 = pd.merge(parallel2, 
                         district_for_route, 
                         on = route_cols,
                         how = "left",
                         validate = "1:1"
                        )
    
    return parallel3


def get_on_shn_routes(date_str: str) -> pd.DataFrame:
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
    # If it was ever tagged as parallel, let's keep that obs
    df2 = (df.groupby(["itp_id", "route_id"])
           .agg({"pct_route": "max", 
                "parallel": "max"})
           .reset_index()
          )[["itp_id", "route_id", "parallel"]].drop_duplicates()
    
    return df2
    
    
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
    
    
def flag_parallel_intersecting_routes(date_str: str) -> pd.DataFrame:
    """
    Take the trips df (each indiv trip) and aggregated trip_service_hrs df
    (aggregated to shape_id), merge together,
    and flag whether a transit route is parallel, on SHN, or other.
    """
    # Merge trips and trips_with_hrs dfs, and aggregate to route_level
    route_level_df = merge_trips_with_service_hrs(date_str)
    
    # Flag routes if they're parallel or on SHN
    parallel_routes = get_parallel_routes(date_str)
    on_shn_routes = get_on_shn_routes(date_str)
    
    # Merge the parallel and on_shn dummy variables in
    with_parallel_flag = pd.merge(route_level_df, 
                                  parallel_routes,
                                  on = ["itp_id", "route_id"],
                                  how = "left",
                                  validate = "1:1"
    )
    
    with_shn_flag = pd.merge(with_parallel_flag,
                             on_shn_routes,
                             on = ["itp_id", "route_id"],
                             how = "left",
                             validate = "1:1"
    )
    
    # Make sure parallel, on_shn, and other are mutually exclusive categories
    # A route can only fall into 1 of these groups
    with_categories = mutually_exclusive_groups(with_shn_flag)
    
    # Flag a unique route, since nunique(route_id) isn't exact, if route_id is 1
    # and many operators share that value
    with_categories = with_categories.assign(unique_route=1)

    return with_categories


#---------------------------------------------------------------#
# Get summary stats (by district) or make bar chart (by district)
#---------------------------------------------------------------#
def add_percent(df: pd.DataFrame, col_list: list) -> pd.DataFrame:
    """
    Create columns with pct values. 
    """
    for c in col_list:
        new_col = f"pct_{c}"
        df[new_col] = (df[c] / df[c].sum()).round(3) * 100
        df[c] = df[c].round(0)
        
    return df


def get_summary_table(df: pd.DataFrame)-> pd.DataFrame: 
    """
    Aggregate by parallel/on_shn/other category.
    Calculate number and pct of service hours, routes.
    """
    summary = geography_utils.aggregate_by_geography(
        df, 
        group_cols = ["category"],
        sum_cols = ["total_service_hours", "unique_route"],
    ).astype({"total_service_hours": int})
    
    summary = add_percent(summary, ["total_service_hours", "unique_route"])
    
    return summary
