"""
Prepare a set of tables to export.

Running this script didn't work, but running it in portions in a notebook did.
TODO: check out why the caching isn't working,
defined filesystem explicitly.
"""

import gcsfs
import google.auth
import hermosa_beach as hb
import pandas as pd
from calitp_data_analysis import utils
from google.cloud import bigquery
from shared_utils import geo_utils

credentials, project = google.auth.default()

PUBLIC_GCS = "gs://calitp-publish-data-analysis/"
HERMOSA_BEACH_GCS = f"{PUBLIC_GCS}hermosa_beach/"

TABLE_KEYS_DICT = {
    "fct_observed_trips": "schedule_base64_url",
    "fct_daily_scheduled_stops": "feed_key",
    "fct_daily_scheduled_shapes": "feed_key",
    "fct_vehicle_locations_path": "schedule_base64_url",
    "fct_vehicle_locations": "base64_url",
}


def basic_sql_query(project_name: str, dataset_name: str, table_name: str) -> str:
    """
    Set up the basic sql query needed, which is the entire table.
    """
    sql_query = f"SELECT * FROM  `{project_name}`.`{dataset_name}`.`{table_name}`"
    return sql_query


def filter_by_date(date_col: str, date_list: str) -> str:
    """
    Add a where condition to filter by date, coerce the dates so sql_query is read correctly.
    Instead of start and end date, try to construct lots of OR statements,
    since having trouble getting WHERE service_date IN (DATE(one_date), DATE(another_date)) to work.
    """
    where_condition = " OR ".join([f"{date_col} = DATE('{d}')" for d in date_list])
    # where_condition = f"{date_col} IN  { tuple({DATE(d)} for d in date_list)} "

    return where_condition


def filter_by_operator_list(gtfs_name_col: str, operator_list: str) -> str:
    """
    Add a where condition to filter for subset of operators.
    Use schedule_gtfs_dataset_name, which can show up as schedule_name or name too in
    various tables.
    """
    where_condition = f"{gtfs_name_col} IN  { tuple(operator_list)} "

    return where_condition


def get_feed_keys_and_schedule_base64_url(trips_file_path: str) -> pd.DataFrame:
    """
    Use scheduled trips to find feed_key or schedule_base64_url that we need.
    Various tables use different partitioning and clustering, so
    need to make use of these.
    """
    df = (
        pd.read_parquet(trips_file_path, columns=["feed_key", "base64_url"], filesystem=gcsfs.GCSFileSystem())
        .drop_duplicates()
        .rename(columns={"base64_url": "schedule_base64_url"})
    )

    return df


def get_vp_base64_url(rt_trips_file_path: str) -> pd.DataFrame:
    """
    Use fct_observed_trips to pare down to the url keys we want.
    """
    df = pd.read_parquet(
        rt_trips_file_path, columns=["schedule_base64_url", "vp_base64_url"], filesystem=gcsfs.GCSFileSystem()
    ).drop_duplicates()

    return df


def download_table(
    sql_query: str,
):
    """
    Download table using google.cloud.bigquery.
    """
    client = bigquery.Client()

    query_job = client.query(sql_query)

    df = query_job.result().to_dataframe()

    if "service_date" in df.columns:
        df = df.astype({"service_date": "datetime64[ns]"})
        # There are several dbdate that comes up in df.dtypes
        # BQ window or intervals, let's remove these

    exclude_me = df.select_dtypes("dbdate").columns
    # some of these show up as datetime, but only for RT tables (tu/vp_trip_start_time_interval)
    interval_cols = [c for c in df.columns if "_interval" in c]
    df = df.drop(columns=exclude_me + interval_cols)

    print(f"executed: {sql_query}")
    return df


def download_schedule_table(table_name: str, date_list: list, operator_list: list, gcs_folder: str):
    """
    Download the simpler tables. Most of these need scheduled trips first,
    then we can use feed_key / schedule_base64_url to key into the other tables.
    Noted the partitions and clusters in hermosa_beach.py, make use of these!
    """
    basic_query = basic_sql_query("cal-itp-data-infra", "mart_gtfs", table_name)
    date_filter_query = filter_by_date("service_date", date_list)

    if table_name == "fct_scheduled_trips":
        operator_filter_query = filter_by_operator_list("name", operator_list)
    else:
        # schedule tables use feed_key to traverse
        # observed trips table uses schedule_base64_url to cluster
        # grab these keys from scheduled trips and use to filter
        feed_key_url_df = get_feed_keys_and_schedule_base64_url(f"{gcs_folder}fct_scheduled_trips.parquet")
        gtfs_name_col = TABLE_KEYS_DICT[table_name]
        operator_filter_query = filter_by_operator_list(gtfs_name_col, feed_key_url_df[gtfs_name_col].unique())

    # all these date filters are or statements, so wrap that up in parentheses
    df = download_table(f"{basic_query} WHERE ({date_filter_query}) AND {operator_filter_query}")

    df.to_parquet(f"{gcs_folder}{table_name}.parquet", filesystem=gcsfs.GCSFileSystem())

    if table_name == "fct_daily_scheduled_stops":
        df = geo_utils.convert_to_gdf(df, "pt_geom", "point")
        utils.geoparquet_gcs_export(df, gcs_folder, table_name, filesystem=gcsfs.GCSFileSystem())

    elif table_name == "fct_daily_scheduled_shapes":
        df = geo_utils.convert_to_gdf(df, "pt_array", "line")
        utils.geoparquet_gcs_export(
            df,
            gcs_folder,
            table_name,
        )

    print(f"saved {gcs_folder}{table_name}.parquet")

    return


def download_vp_tables(table_name: str, date_list: list, operator_list: list, gcs_folder: str):
    """
    Download vp tables.
    This one relies on using fct_vp_path first (which does have service_date), then we can get the base64_urls we want,
    and filter on service_date (but keep dt).
    fct_vehicle_locations is partitioned on dt, which can and will differ from service_date due to UTC to pacific conversions.
    """
    subset_urls_df = get_vp_base64_url(f"{gcs_folder}fct_observed_trips.parquet")

    basic_query = basic_sql_query("cal-itp-data-infra", "mart_gtfs", table_name)
    date_filter_query = filter_by_date("service_date", date_list)
    gtfs_name_col = TABLE_KEYS_DICT[table_name]
    operator_filter_query = filter_by_operator_list(gtfs_name_col, subset_urls_df[gtfs_name_col].unique())

    if table_name == "fct_vehicle_locations_path":
        df = download_table(f"{basic_query} WHERE ({date_filter_query}) AND {operator_filter_query}")

        df.to_parquet(f"{gcs_folder}{table_name}.parquet", filesystem=gcsfs.GCSFileSystem())

    return


if __name__ == "main":

    for one_table in hb.GTFS_MART_TABLES:
        download_schedule_table(one_table, hb.HERMOSA_BEACH_DATES, hb.SUBSET_OF_OPERATORS, HERMOSA_BEACH_GCS)

    for one_table in hb.VP_TABLES:
        download_vp_tables(one_table, hb.HERMOSA_BEACH_DATES, hb.SUBSET_OF_OPERATORS, HERMOSA_BEACH_GCS)
