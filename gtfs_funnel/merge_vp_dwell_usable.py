import datetime
import numpy as np
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils.project_vars import SEGMENT_GCS
from shared_utils import publish_utils
from update_vars import GTFS_DATA_DICT
from vp_dwell_time import import_vp


def merge_vp_usable_dwell(vp_with_dwell: pd.DataFrame) -> pd.DataFrame:
    remaining_vp = vp_with_dwell.vp_idx.tolist()

    vp_usable = import_vp(analysis_date, filters=[[("vp_idx", "in", remaining_vp)]])
    print("TODO: Remove these")
    print("usable")
    print(vp_usable.head())
    print("with dwell")
    print(vp_with_dwell.head())

    vp_usable_with_dwell = (
        pd.merge(vp_usable, vp_with_dwell, on="vp_idx", how="inner")
        .sort_values("vp_idx")
        .reset_index(drop=True)
    )
    return vp_usable_with_dwell


if __name__ == "__main__":
    from update_vars import analysis_date_list

    UNMERGED_VP_DWELL = GTFS_DATA_DICT.speeds_tables.vp_dwell_premerge
    EXPORT_FILE = GTFS_DATA_DICT.speeds_tables.vp_dwell
    LOG_FILE = "./logs/vp_preprocessing.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(
        sys.stderr,
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
        level="INFO",
    )

    start = datetime.datetime.now()
    for analysis_date in analysis_date_list:
        vp_with_dwell = pd.read_parquet(
            f"{SEGMENT_GCS}{UNMERGED_VP_DWELL}_{analysis_date}.parquet",
        )

        vp_usable_with_dwell = merge_vp_usable_dwell(vp_with_dwell)
        publish_utils.if_exists_then_delete(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}"
        )

        vp_usable_with_dwell.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}",
            partition_cols="gtfs_dataset_key",
        )

    end = datetime.datetime.now()

    logger.info(f"merge with original and export: {end - start}")
