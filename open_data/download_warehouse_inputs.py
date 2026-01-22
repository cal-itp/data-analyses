"""
Download fct_monthly_routes (shapes),
fct_monthly_scheduled_stops (stops),
see if we need fct_monthly_scheduled_trips.
"""

import datetime
import sys

import gcsfs
from calitp_data_analysis import utils
from loguru import logger
from shared_utils import bq_utils
from update_vars import OPEN_DATA_GCS, analysis_month

PROD_PROJECT = "cal-itp-data-infra"
PROD_MART = "mart_gtfs_rollup"

if __name__ == "__main__":

    LOG_FILE = "./logs/open_data.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")
    
    MONTH_DATE_COL = "month_first_day"
    start = datetime.datetime.now()

    # Download stops
    monthly_stops = bq_utils.download_table(
        project_name=PROD_PROJECT,
        dataset_name=PROD_MART,
        table_name="fct_monthly_scheduled_stops",
        date_col=MONTH_DATE_COL,
        start_date=analysis_month,
        end_date=analysis_month,
        geom_col="pt_geom",
        geom_type="point",
    )

    utils.geoparquet_gcs_export(monthly_stops, OPEN_DATA_GCS, f"stops_{analysis_month}")

    t1 = datetime.datetime.now()
    logger.info(f"stops: {analysis_month}: {t1 - start}")

    # Download shapes with route_info
    monthly_routes = bq_utils.download_table(
        project_name=PROD_PROJECT,
        dataset_name=PROD_MART,
        table_name="fct_monthly_routes",
        date_col=MONTH_DATE_COL,
        start_date=analysis_month,
        end_date=analysis_month,
        geom_col="pt_array",
        geom_type="line",
    )

    utils.geoparquet_gcs_export(monthly_routes, OPEN_DATA_GCS, f"routes_{analysis_month}")
    t2 = datetime.datetime.now()
    logger.info(f"routes: {analysis_month}: {t2 - t1}")
    
    crosswalk = bq_utils.download_table(
        project_name=PROD_PROJECT,
        dataset_name="mart_transit_database",
        table_name="bridge_gtfs_analysis_name_x_ntd",
        date_col=None,
    )

    crosswalk.to_parquet(f"{OPEN_DATA_GCS}bridge_gtfs_analysis_name_x_ntd.parquet", filesystem=gcsfs.GCSFileSystem())

    end = datetime.datetime.now()
    logger.info(f"crosswalk: {end - t2}")
    logger.info(f"execution time: {analysis_month}: {end - start}")
