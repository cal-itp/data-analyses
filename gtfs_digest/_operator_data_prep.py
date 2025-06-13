"""
Produce the aggregation for section 1 (operator)
in GTFS digest and filter to only recent date.

Includes operator info and operator route map.
"""
import datetime
import geopandas as gpd
import google.auth
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from shared_utils import publish_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS

credentials, project = google.auth.default()

routes_readable_col_names = {
    "recent_combined_name":"Route",
    "route_length_miles": "Route Length (Miles)",
    "percentile_group":"Percentile Group",
    "route_length_miles_percentile":"Route Length Percentile Groups",
    "route_id": "Route ID",
}

routes_subset = [
    "geometry",
    "route_id",
    "route_length_miles",
    "is_downtown_local",
    "is_local",
    "is_coverage",
    "is_rapid",
    "is_express",
    "is_rail",
    "is_ferry",
    "service_date",
    "portfolio_organization_name",
    "recent_combined_name",
    "route_length_miles_percentile",
    "percentile_group",
]

operator_date_cols = ["portfolio_organization_name", "service_date"]


def aggregate_operator_stats(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Aggregate by portfolio organization name
    and get operator profile metrics.
    Most of these are sums.
    """
    gtfs_cols = [
        f"operator_n_{i}" for i in 
        ["routes", "trips", "shapes", "stops", "arrivals"]
    ] + ["operator_route_length_miles"]
    
    route_typology_cols = [
        f"n_{i}_routes" for i in 
        ["downtown_local", "local", "coverage", "rapid", "express", "rail", "ferry"]
    ]
    
    # These columns either needed weighted average
    # or display both, like ntd, or be generous and display the larger value across feeds
    weighted_avg_cols = ["vp_per_min_agency", "spatial_accuracy_agency"]
    
    agg1 = (
        df
        .groupby(group_cols, group_keys=False)
        .agg({
            **{c: "sum" for c in gtfs_cols + route_typology_cols},
            **{c: "max" for c in weighted_avg_cols},
            **{"schedule_gtfs_dataset_key": "nunique"},
            **{"name": lambda x: list(x)},
            **{"counties_served": "first"}
        }).reset_index()
        .rename(columns = {
            "schedule_gtfs_dataset_key": "n_feeds",
            "name": "operator_feeds"
        })
    ).pipe(list_pop_as_string, ["counties_served", "operator_feeds"])
    
    # Pop out the operator gtfs_dataset_names 
    agg1['operator_feeds'] = agg1['operator_feeds'].apply(
        lambda x: ', '.join(set(x.split(', ')))
    )
    
    return agg1
    

def list_pop_as_string(
    df: pd.DataFrame, 
    stringify_cols: list
) -> pd.DataFrame:
    # pop list as string, so instead of [one, two], we can display "one, two"
    for c in stringify_cols:
        df[c] = df[c].apply(lambda x: ', '.join(map(str, x)))

    return df


def unpack_multiple_ntd(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Test a function that allows multiple ntd entries, like hq_city, primary_uza, etc.
    Unpack these as a string to populate description.
    Don't think we want to get rid of multiple ntd_ids...if an operator can be associated
    with multiple entries, we should unpack as much as we can? unless we decide to set a primary,
    which is dependent on knowing operator-organization-ntd_id relationship.
    """
    # Group by name-service_date-portfolio_organization_name to aggregate up to 
    # portfolio_organization_name,because name indicates different feeds, so we want to sum those.
    agg1 = (
        df
        # sometimes ntd info is None, drop these if it's none across all 3 columns
        .dropna(subset=["hq_city", "reporter_type", "primary_uza_name"])
        .groupby(group_cols, group_keys=False)
        .agg({
            "service_area_pop": "sum", 
            "service_area_sq_miles": "sum",
            # do not sort here because we might scramble them?
            "hq_city": lambda x: list(set(x)),
            "reporter_type": lambda x: list(set(x)), 
            "primary_uza_name": lambda x: list(set(x)),
        })
        .reset_index()
    ).pipe(
        list_pop_as_string, 
        ["hq_city", "reporter_type", 
         "primary_uza_name"]
    )
    
    return agg1

if __name__ == "__main__":
    
    logger.add("./logs/digest_data_prep.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map
    
    OPERATOR_PROFILE_REPORT = GTFS_DATA_DICT.digest_tables.operator_profiles_report
    OPERATOR_ROUTE_REPORT = GTFS_DATA_DICT.digest_tables.operator_routes_map_report
    
    most_recent_routes = gpd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_ROUTE}.parquet",
        columns = routes_subset,
        storage_options={"token": credentials.token},
    ).pipe(
        publish_utils.filter_to_recent_date,
        ["portfolio_organization_name"]
    ).drop_duplicates(
        subset = ["portfolio_organization_name", "route_id"]
    ).rename(
        columns = routes_readable_col_names
    ).reset_index(drop=True)
    
    utils.geoparquet_gcs_export(
        most_recent_routes,
        RT_SCHED_GCS,
        OPERATOR_ROUTE_REPORT
    )
    
    # For each date, get aggregated operator stats (count route typologies, etc)
    # and NTD stats, then filter to the most recent date for the report
    operator_data = pd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}.parquet"
    )
    
    operator_aggregated = aggregate_operator_stats(
        operator_data,
        group_cols = operator_date_cols + ["caltrans_district"]
    )
    
    ntd_data = unpack_multiple_ntd(
        operator_data,
        group_cols = operator_date_cols
    )
    
    most_recent_operator_data = pd.merge(
        operator_aggregated,
        ntd_data,
        on = operator_date_cols,
        how = "inner"
    ).pipe(
        publish_utils.filter_to_recent_date, 
        ["portfolio_organization_name"]
    ).rename(
        columns = routes_readable_col_names
    )
    
    most_recent_operator_data.to_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE_REPORT}.parquet"
    )
    
    end = datetime.datetime.now()
    logger.info(f"operator viz data prep: {end - start}")
