"""
Download all stop_times for a day.
"""

import os

os.environ["CALITP_BQ_MAX_BYTES"] = str(500_000_000_000)

import datetime as dt  # noqa: E402
import sys  # noqa: E402
from functools import cache  # noqa: E402

from calitp_data_analysis.gcs_pandas import GCSPandas  # noqa: E402
from loguru import logger  # noqa: E402
from shared_utils import gtfs_utils_v2  # noqa: E402
from update_vars import COMPILED_CACHED_VIEWS, GTFS_DATA_DICT  # noqa: E402


@cache
def gcs_pandas():
    return GCSPandas()


def download_one_day(analysis_date: str):
    """
    Download single day for stop_times.
    """
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()

    full_trips = gcs_pandas().read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet",
    )

    FEEDS_TO_RUN = full_trips.feed_key.unique().tolist()

    logger.info(f"# operators to run: {len(FEEDS_TO_RUN)}")

    # st already used, keep for continuity
    dataset = GTFS_DATA_DICT.schedule_downloads.stop_times
    logger.info(f"*********** Download {dataset} data ***********")

    keep_stop_time_cols = [
        "feed_key",
        "feed_timezone",
        "base64_url",
        "trip_id",
        "stop_id",
        "stop_sequence",
        "timepoint",
        "arrival_sec",
        "departure_sec",
    ]

    stop_times = gtfs_utils_v2.get_stop_times(
        selected_date=analysis_date,
        operator_feeds=FEEDS_TO_RUN,
        stop_time_cols=keep_stop_time_cols,
        get_df=True,
        trip_df=full_trips,
    )

    gcs_pandas().data_frame_to_parquet(stop_times, f"{COMPILED_CACHED_VIEWS}{dataset}_{analysis_date}.parquet")

    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

    return


if __name__ == "__main__":

    from update_vars import analysis_date_list

    logger.add("./logs/download_data.log", retention="3 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    for analysis_date in analysis_date_list:
        download_one_day(analysis_date)
