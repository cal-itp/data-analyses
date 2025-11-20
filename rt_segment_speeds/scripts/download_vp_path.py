"""
Download vp_path.
"""

import datetime
import sys

import google.auth
from google.cloud import bigquery
from loguru import logger
from shared_utils import rt_dates
from update_vars import SEGMENT_GCS

credentials, project = google.auth.default()


def download_vp_path(table_name="fct_vehicle_locations_path", analysis_date: str = "", geom_col: str = "pt_array"):
    """
    Download a single day's worth of vp path
    """
    t0 = datetime.datetime.now()

    sql_query = f"""
        SELECT *
        FROM  `cal-itp-data-infra`.`mart_gtfs`.`{table_name}`
        WHERE service_date = DATE('{analysis_date}')

    """
    client = bigquery.Client()

    query_job = client.query(sql_query)

    df = query_job.result().to_dataframe()

    df.to_parquet(f"{SEGMENT_GCS}{table_name}_{analysis_date}.parquet")
    t1 = datetime.datetime.now()
    logger.info(f"download {table_name}: {analysis_date}: {t1 - t0}")

    return


if __name__ == "__main__":

    LOG_FILE = "../logs/movingpandas_pipeline.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    analysis_date_list = [rt_dates.DATES[f"{m}2025"] for m in ["aug"]]

    for analysis_date in analysis_date_list:
        download_vp_path(
            table_name="fct_vehicle_locations_path",
            analysis_date=analysis_date,
        )
