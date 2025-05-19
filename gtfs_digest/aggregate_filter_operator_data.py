"""
Produce the aggregation for section 1 (operator)
in GTFS digest and filter to only recent date.

Includes operator info and operator route map.

TODO: add renaming and any remaining viz wrangling
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from shared_utils import publish_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS

def list_pop_as_string(df, stringify_cols: list):
    # pop list as string, so instead of [one, two], we can display "one, two"
    for c in stringify_cols:
        df[c] = df[c].apply(lambda x: ', '.join(map(str, x)))

    return df


def unpack_multiple_ntd(df: pd.DataFrame) -> pd.DataFrame:
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
        .groupby(
            [
                "service_date",
                "portfolio_organization_name",
                "caltrans_district",
            ]
        )
        .agg({
            "service_area_pop": "sum", 
            "service_area_sq_miles": "sum",
            # do not sort here because we might scramble them?
            "hq_city": lambda x: list(set(x)),
            "reporter_type": lambda x: list(set(x)), 
            "primary_uza_name": lambda x: list(set(x)),
        })
        .reset_index()
    ).pipe(list_pop_as_string, ["hq_city", "reporter_type", "primary_uza_name"])
    
    
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
    
    most_recent_dates = publish_utils.filter_to_recent_date(
        operator_data,
        ["portfolio_organization_name"]
    )
    
    most_recent_operator_data = pd.merge(
        operator_data,
        most_recent_dates,
        on = ["portfolio_organization_name", "service_date"],
        how = "inner"
    ).pipe(unpack_multiple_ntd)
    
    most_recent_operator_data.to_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}_recent.parquet"
    )