import _sql_query
import pandas as pd
from shared_utils import portfolio_utils
from update_vars import GTFS_DATA_DICT

def download_schedule_rt_route_direction_summary() -> pd.DataFrame:
    df = _sql_query.download_with_pandas_gbq(
        project="cal-itp-data-infra-staging",
        filename="tiffany_mart_gtfs_rollup.fct_monthly_schedule_rt_route_direction_summary",
    )

    # Save to our private GCS
    df.to_parquet(
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/schedule_rt_route_direction_summary.parquet"
    )

    return df


def download_operator_summary() -> pd.DataFrame:
    df = _sql_query.download_with_pandas_gbq(
        project="cal-itp-data-infra-staging",
        filename="tiffany_mart_gtfs_rollup.fct_monthly_operator_summary",
    )

    # Save to our private GCS
    df.to_parquet(
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/fct_monthly_operator_summary.parquet"
    )
    return df


def download_fct_monthly_routes() -> pd.DataFrame:
    df = _sql_query.download_with_pandas_gbq(
        project="cal-itp-data-infra-staging",
        filename="tiffany_mart_gtfs_rollup.fct_monthly_routes",
    )

    # Save to our private GCS
    df.to_parquet(
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/fct_monthly_routes.parquet"
    )
    return df


def download_fct_operator_hourly_summary() -> pd.DataFrame:
    df = _sql_query.download_with_pandas_gbq(
        project="cal-itp-data-infra-staging",
        filename="tiffany_mart_gtfs_rollup.fct_operator_hourly_summary",
    )

    # Save to our private GCS
    df.to_parquet(
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/fct_operator_hourly_summary.parquet"
    )
    return df