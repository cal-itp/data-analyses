"""
Download tables for creating RT stop, route, operator report.
"""

import datetime
import sys

import google.auth
from calitp_data_analysis import utils
from loguru import logger
from rt_msa_utils import PREDICTIONS_GCS
from shared_utils import bq_utils

credentials, project = google.auth.default()

PRODUCTION_PROJECT = "cal-itp-data-infra"
PRODUCTION_MART_GTFS = "mart_gtfs"
PRODUCTION_MART_ROLLUP = "mart_gtfs_rollup"

if __name__ == "__main__":

    LOG_FILE = "./logs/download_warehouse.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    # (1) Stop report tables
    start = datetime.datetime.now()

    monthly_stops = bq_utils.download_table(
        project_name="cal-itp-data-infra-staging",
        dataset_name="tiffany_mart_gtfs_rollup",
        table_name="test_monthly_schedule_rt_stops",
        date_col="month_first_day",
        start_date="2025-12-01",
        end_date="2026-01-01",
        geom_col="pt_geom",
        geom_type="point",
    )

    utils.geoparquet_gcs_export(monthly_stops, PREDICTIONS_GCS, "monthly_schedule_rt_stops")
    end = datetime.datetime.now()
