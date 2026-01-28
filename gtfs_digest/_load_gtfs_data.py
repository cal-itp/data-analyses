from functools import cache
from pathlib import Path

import pandas as pd
import google.auth

from calitp_data_analysis.gcs_pandas import GCSPandas
from calitp_data_analysis import utils
from calitp_data_analysis.sql import get_engine

from shared_utils import bq_utils
from update_vars import GTFS_DATA_DICT, analysis_month, file_name, last_year, previous_month

import _prep_crosswalk_ntd  # needed for load_crosswalk()

# Initialize credentials and DB engine
credentials, project = google.auth.default()
db_engine = get_engine()


@cache
def gcs_pandas():
    return GCSPandas()


def load_schedule_rt_route_direction_summary(
    project_name: str,
    date_col: str,
    dataset_name: str,
    start_date: str,
    end_date: str,
    file_name: str,
) -> pd.DataFrame:
    df = bq_utils.download_table(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=GTFS_DATA_DICT.gtfs_digest_rollup.schedule_rt_route_direction,
        date_col=date_col,
        start_date=start_date,
        end_date=end_date,
    )

    # Merge with crosswalk
    crosswalk_url = f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk}_{file_name}.parquet"

    crosswalk_df = gcs_pandas().read_parquet(crosswalk_url)[["name", "analysis_name"]]
    
    m1 = pd.merge(df, crosswalk_df, on="name", how="inner")

    gcs_pandas().data_frame_to_parquet(
        m1,
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/"
        f"{GTFS_DATA_DICT.gtfs_digest_rollup.schedule_rt_route_direction}_{file_name}.parquet"
    )

    return m1


def load_operator_summary(
    project_name: str,
    date_col: str,
    dataset_name: str,
    start_date: str,
    end_date: str,
    file_name: str,
) -> pd.DataFrame:
    df = bq_utils.download_table(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=GTFS_DATA_DICT.gtfs_digest_rollup.operator_summary,
        date_col=date_col,
        start_date=start_date,
        end_date=end_date,
    )

    crosswalk_url = f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk}_{file_name}.parquet"

    crosswalk_df = gcs_pandas().read_parquet(crosswalk_url)[["name","analysis_name","caltrans_district"]]
    
    m1 = pd.merge(df, crosswalk_df, left_on = ["schedule_name"], right_on = ["name"], how="inner")

    gcs_pandas().data_frame_to_parquet(
        m1,
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/"
        f"{GTFS_DATA_DICT.gtfs_digest_rollup.operator_summary}_{file_name}.parquet"
    )

    return m1


def load_fct_monthly_routes(
    project_name: str,
    date_col: str,
    dataset_name: str,
    start_date: str,
    end_date: str,
    file_name: str,
) -> pd.DataFrame:
    gdf = bq_utils.download_table(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=GTFS_DATA_DICT.gtfs_digest_rollup.route_map,
        date_col=date_col,
        start_date=start_date,
        end_date=end_date,
        geom_col="pt_array",
        geom_type="line",
    )

    crosswalk_url = f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk}_{file_name}.parquet"

    crosswalk_df = gcs_pandas().read_parquet(crosswalk_url)[["name", "analysis_name", "caltrans_district"]]
    
    m1 = pd.merge(gdf, crosswalk_df, on="name", how="inner")

    utils.geoparquet_gcs_export(
        gdf=m1,
        gcs_file_path=f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/",
        file_name=f"{GTFS_DATA_DICT.gtfs_digest_rollup.route_map}_{file_name}",
    )

    return m1


def load_fct_operator_hourly_summary(
    project_name: str,
    date_col: str,
    dataset_name: str,
    start_date: str,
    end_date: str,
    file_name: str,
) -> pd.DataFrame:
    df = bq_utils.download_table(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=GTFS_DATA_DICT.gtfs_digest_rollup.hourly_day_type_summary,
        date_col=date_col,
        start_date=start_date,
        end_date=end_date,
    )

    # Merge with crosswalk
    crosswalk_url = f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk}_{file_name}.parquet"

    crosswalk_df = gcs_pandas().read_parquet(crosswalk_url)[["name", "analysis_name",]]

    m1 = pd.merge(df, crosswalk_df, on="name", how="inner").drop_duplicates().reset_index()
    
    gcs_pandas().data_frame_to_parquet(m1, f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/{GTFS_DATA_DICT.gtfs_digest_rollup.hourly_day_type_summary}_{file_name}.parquet")
   
    return m1

    
if __name__ == "__main__":
    PROD_PROJECT = "cal-itp-data-infra"
    PROD_MART = "mart_gtfs_rollup"
    MONTH_DATE_COL = "month_first_day"

    schedule_rt_route_direction_summary = load_schedule_rt_route_direction_summary(
        project_name=PROD_PROJECT,
        date_col=MONTH_DATE_COL,
        dataset_name=PROD_MART,
        start_date=last_year,
        end_date=analysis_month,
        file_name=file_name,
    )

    monthly_operator_summary_df = load_operator_summary(
        project_name=PROD_PROJECT,
        date_col=MONTH_DATE_COL,
        dataset_name=PROD_MART,
        start_date=last_year,
        end_date=analysis_month,
        file_name=file_name,
    )

    monthly_routes_gdf = load_fct_monthly_routes(
        project_name=PROD_PROJECT,
        date_col=MONTH_DATE_COL,
        dataset_name=PROD_MART,
        start_date=previous_month,
        end_date=analysis_month,
        file_name=file_name,
    )

    fct_operator_hourly_summary = load_fct_operator_hourly_summary(
        project_name=PROD_PROJECT,
        date_col=MONTH_DATE_COL,
        dataset_name=PROD_MART,
        start_date=last_year,
        end_date=analysis_month,
        file_name=file_name,
    )


