"""
Produce the aggregation for section 1 (operator)
in GTFS digest and filter to only recent date.

Includes operator info and operator route map.

TODO: add renaming and any remaining viz wrangling.
This script is equivalent to viz_data_prep.py, so both
should be renamed to make grain clear, and do similar things between route-dir
and operator grain.
(aggregate metrics, rename for viz)
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from shared_utils import publish_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS

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
    
    return agg1
    
    
def renaming_stuff(df):
    # Add renaming step similar to viz_data_prep
    
    # probably want to drop columns that aren't applicable
    # organization_source_record_id, organization_name
    return

def list_pop_as_string(df, stringify_cols: list):
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
    
    OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map
    
    
    operator_routes = gpd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_ROUTE}.parquet"
    )
    
    # Maybe rewrite this function to return the subset df
    most_recent_dates = publish_utils.filter_to_recent_date(
        operator_routes,
        ["portfolio_organization_name"]
    )
    
    most_recent_routes = pd.merge(
        operator_routes,
        most_recent_dates,
        on = ["portfolio_organization_name", "service_date"],
        how = "inner"
    )
    
    utils.geoparquet_gcs_export(
        most_recent_routes,
        RT_SCHED_GCS,
        f"{OPERATOR_ROUTE}_recent"
    )
    
    operator_data = pd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}.parquet"
    )
    
    operator_aggregated = aggregate_operator_stats(
        operator_data,
        group_cols = ["portfolio_organization_name", "service_date", "caltrans_district"]
    )
    
    ntd_data = unpack_multiple_ntd(
        operator_data,
        group_cols = ["portfolio_organization_name", "service_date"]
    )

    most_recent_dates = publish_utils.filter_to_recent_date(
        operator_data,
        ["portfolio_organization_name"]
    )
    
    most_recent_operator_data = pd.merge(
        operator_aggregated,
        most_recent_dates,
        on = ["portfolio_organization_name", "service_date"],
        how = "inner"
    ).merge(
        ntd_data,
        on = ["portfolio_organization_name", "service_date"],
        how = "inner"
    )
    
    most_recent_operator_data.to_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}_recent.parquet"
    )