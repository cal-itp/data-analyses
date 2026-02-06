"""
Download all stops for a day.
"""

import os

os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

import datetime as dt  # noqa: E402
import sys  # noqa: E402

from calitp_data_analysis import geography_utils, utils  # noqa: E402
from download_trips import get_operators  # noqa: E402
from loguru import logger  # noqa: E402
from shared_utils import gtfs_utils_v2  # noqa: E402
from update_vars import COMPILED_CACHED_VIEWS, GTFS_DATA_DICT  # noqa: E402


def download_one_day(analysis_date: str):
    """
    Download single day for stops.
    """
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()

    operators_df = get_operators(analysis_date)

    FEEDS_TO_RUN = sorted(operators_df.feed_key.unique().tolist())

    logger.info(f"# operators to run: {len(FEEDS_TO_RUN)}")

    dataset = GTFS_DATA_DICT.schedule_downloads.stops

    logger.info(f"*********** Download {dataset} data ***********")

    # keep all the route_types, use for filtering for rail/brt/ferry later
    route_type_vals = [0, 1, 2, 3, 4, 5, 6, 7, 11, 12]
    route_type_cols = [f"route_type_{i}" for i in route_type_vals]

    keep_stop_cols = (
        [
            "feed_key",
            "service_date",
            "feed_timezone",
            "first_stop_arrival_datetime_pacific",
            "last_stop_departure_datetime_pacific",
            "stop_id",
            "stop_key",
            "stop_name",
            "daily_arrivals",
            "parent_station",
            "location_type",
        ]
        + route_type_cols
        + ["missing_route_type"]
    )

    stops = gtfs_utils_v2.get_stops(
        selected_date=analysis_date,
        operator_feeds=FEEDS_TO_RUN,
        stop_cols=keep_stop_cols,
        get_df=True,
        crs=geography_utils.WGS84,
    )

    utils.geoparquet_gcs_export(stops, COMPILED_CACHED_VIEWS, f"{dataset}_{analysis_date}.parquet")

    end = dt.datetime.now()
    logger.info(f"execution time: {end - start}")

    return


if __name__ == "__main__":

    from update_vars import analysis_date_list

    logger.add("./logs/download_data.log", retention="3 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    for analysis_date in analysis_date_list:
        download_one_day(analysis_date)
