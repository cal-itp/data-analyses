"""
Download vehicle positions for a day.
"""

import datetime
import os
import sys

import gcsfs
import pandas as pd
from loguru import logger

#  CALITP_BQ_MAX_BYTES needs to be set before importing DBSession, which uses it via calitp_data_analysis.sql.get_engine
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)
from shared_utils import DBSession, schedule_rt_utils  # noqa: E402
from shared_utils.models.fct_vehicle_location import FctVehicleLocation  # noqa: E402
from sqlalchemy import and_, select  # noqa: E402
from update_vars import SEGMENT_GCS  # noqa: E402

os.environ["USE_PYGEOS"] = "0"

fs = gcsfs.GCSFileSystem()


def determine_batches(rt_names: list) -> dict:
    # https://stackoverflow.com/questions/4843158/how-to-check-if-a-string-is-a-substring-of-items-in-a-list-of-strings
    la_metro_names = [
        "LA Metro Bus",
        "LA Metro Rail",
    ]

    bay_area_large_names = ["AC Transit", "Muni"]

    bay_area_names = ["Bay Area 511"]

    # If any of the large operator name substring is
    # found in our list of names, grab those
    # be flexible bc "Vehicle Positions" and "VehiclePositions" present
    matching1 = [i for i in rt_names if any(name in i for name in la_metro_names)]

    matching2 = [i for i in rt_names if any(name in i for name in bay_area_large_names)]

    remaining_bay_area = [
        i
        for i in rt_names
        if any(name in i for name in bay_area_names) and (i not in matching1) and (i not in matching2)
    ]
    remaining = [i for i in rt_names if (i not in matching1) and (i not in matching2) and (i not in remaining_bay_area)]

    # Batch large operators together and run remaining in 2nd query
    batch_dict = {}

    batch_dict[0] = matching1
    batch_dict[1] = remaining_bay_area
    batch_dict[2] = remaining
    batch_dict[3] = matching2

    return batch_dict


def download_vehicle_positions(date: str, operator_names: list) -> pd.DataFrame:
    statement = select(
        FctVehicleLocation.gtfs_dataset_key,
        FctVehicleLocation.gtfs_dataset_name,
        FctVehicleLocation.schedule_gtfs_dataset_key,
        FctVehicleLocation.trip_id,
        FctVehicleLocation.trip_instance_key,
        FctVehicleLocation.location_timestamp,
        FctVehicleLocation.location,
    ).where(and_(FctVehicleLocation.service_date == date, FctVehicleLocation.gtfs_dataset_name.in_(operator_names)))

    with DBSession() as session:
        df = pd.read_sql(statement, session.bind)

    return df


def loop_through_batches_and_download_vp(batches: dict, analysis_date: str):
    """
    Loop through batches dictionary and download vehicle positions.
    Download for that batch of operators, for that date.
    """
    for i, subset_operators in batches.items():
        time0 = datetime.datetime.now()

        df = download_vehicle_positions(analysis_date, subset_operators)

        df.to_parquet(f"{SEGMENT_GCS}vp_raw_{analysis_date}_batch{i}.parquet")

        del df

        time1 = datetime.datetime.now()
        logger.info(f"exported batch {i} to GCS: {time1 - time0}")


if __name__ == "__main__":

    from update_vars import analysis_date_list

    # from dask.distributed import Client
    # client = Client("dask-scheduler.dask.svc.cluster.local:8786")

    LOG_FILE = "./logs/download_vp_v2.log"

    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    # Get rt_datasets that are available for that day
    rt_datasets = schedule_rt_utils.filter_dim_gtfs_datasets(
        keep_cols=["key", "name", "type", "regional_feed_type"],
        custom_filtering={"type": ["vehicle_positions"]},
        get_df=True,
    )

    rt_datasets = rt_datasets.rename(columns={"gtfs_dataset_name": "name"})

    # Exclude regional feed and precursors
    exclude = ["Bay Area 511 Regional VehiclePositions"]
    rt_datasets = rt_datasets[
        ~(rt_datasets.name.isin(exclude)) & (rt_datasets.regional_feed_type != "Regional Precursor Feed")
    ].reset_index(drop=True)

    rt_dataset_names = rt_datasets.name.unique().tolist()
    batches = determine_batches(rt_dataset_names)

    for analysis_date in analysis_date_list:
        logger.info(f"Analysis date: {analysis_date}")

        start = datetime.datetime.now()

        loop_through_batches_and_download_vp(batches, analysis_date)

        end = datetime.datetime.now()
        logger.info(f"execution time: {end - start}")

    # client.close()
