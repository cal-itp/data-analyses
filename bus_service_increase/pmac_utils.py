"""
Functions to wrangle PMAC datasets created from D1_pmac_routes.py
"""
import altair as alt
import geopandas as gpd
import pandas as pd

from D1_pmac_routes import COMPILED_CACHED_GCS
from utils import GCS_FILE_PATH
from shared_utils import geography_utils

route_cols = ["itp_id", "route_id"]
#---------------------------------------------------------------#
# Data processing - merge trips dfs, tag a route as parallel/on shn/other
#---------------------------------------------------------------#

def calculate_route_level_service_hours(date_str) -> pd.DataFrame:
    """
    Merge trips df with trips_with_service_hrs (aggregated to shape_id).
    
    Aggregate this to route_level, since shape_id is confusing
    and leads to double-counting, or merges not going through in full.
    """
    trips_with_hrs = pd.read_parquet(
        f"{GCS_FILE_PATH}trips_with_hrs_{date_str}.parquet").rename(
        columns = {"calitp_itp_id": "itp_id"})
    
    trips = pd.read_parquet(
        f"{COMPILED_CACHED_GCS}trips_{date_str}.parquet").rename(
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

    
def get_parallel_routes(date_str: str) -> pd.DataFrame:
     # If it is parallel, we want to flag as 1
    parallel = gpd.read_parquet(
        f"{GCS_FILE_PATH}parallel_or_intersecting_{date_str}.parquet")
    
    parallel = parallel[parallel.parallel==1]
    
    parallel2 = get_unique_routes(parallel)
    
    return parallel2


def get_on_shn_routes(date_str: str) -> pd.DataFrame:
    # These are routes that have some part on SHN
    # BUT, there is overlap between the parallel
    # Since the requirements here are less stringent than parallel
    # So, remove those that are already parallel
    on_shn = gpd.read_parquet(
        f"{GCS_FILE_PATH}routes_on_shn_{date_str}.parquet"
    )
    on_shn2 = on_shn[on_shn.parallel == 1]

    on_shn3 = get_unique_routes(on_shn2)
    
    return on_shn3


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
    def make_mutually_exclusive(row) -> str:
        if row.in_parallel=="both":
            return "parallel"
        elif (row.in_parallel=="left_only") and (row.in_on_shn=="both"):
            return "on_shn"
        elif (row.in_parallel=="left_only") and (row.in_on_shn=="left_only"):
            return "other"
    
    df2 = df.assign(
        category = df.apply(lambda x: make_mutually_exclusive(x), axis=1),
        # Flag a unique route, since nunique(route_id) isn't exact, if route_id is 1
        # and many operators share that value
        unique_route = 1
    ).drop(columns = ["in_parallel", "in_on_shn"])
    
    return df2
    

def add_district(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    Merge in district info (only 1 district per route_id)
    """
    
    parallel = gpd.read_parquet(
        f"{GCS_FILE_PATH}parallel_or_intersecting_{date_str}.parquet")
    
    district = parallel[route_cols + ["pct_route","District"]].drop_duplicates()
    
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
    )
    
    return df2

    
def flag_parallel_intersecting_routes(date_str: str) -> pd.DataFrame:
    """
    Take the trips df (each indiv trip) and aggregated trip_service_hrs df
    (aggregated to shape_id), merge together,
    and flag whether a transit route is parallel, on SHN, or other.
    """
    # Merge trips and trips_with_hrs dfs, and aggregate to route_level
    route_level_df = calculate_route_level_service_hours(date_str)
    
    # Flag routes if they're parallel or on SHN
    parallel_routes = get_parallel_routes(date_str)
    on_shn_routes = get_on_shn_routes(date_str)
    
    # Merge the parallel and on_shn dummy variables in
    with_parallel_flag = pd.merge(route_level_df, 
                                  parallel_routes,
                                  on = route_cols,
                                  how = "left",
                                  validate = "1:1",
                                  indicator="in_parallel"
    )
    
    with_shn_flag = pd.merge(with_parallel_flag,
                             on_shn_routes,
                             on = route_cols,
                             how = "left",
                             validate = "1:1",
                             indicator="in_on_shn"
    )
    

    # Make sure parallel, on_shn, and other are mutually exclusive categories
    # A route can only fall into 1 of these groups
    with_categories = mutually_exclusive_groups(with_shn_flag)
    with_categories = add_district(with_categories, date_str)
    
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

#https://stackoverflow.com/questions/23482668/sorting-by-a-custom-list-in-pandas
def sort_by_column(df: pd.DataFrame, 
                   col: str = "category", 
                   sort_key: list = ["parallel", "on_shn", "other"]
                  ) -> pd.DataFrame:
    # Custom sort order for categorical variable
    df = df.sort_values(col, 
                        key=lambda c: c.map(lambda e: sort_key.index(e)))
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
    
    summary = sort_by_column(summary)
    
    return summary
