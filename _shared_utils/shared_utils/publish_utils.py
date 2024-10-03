import os
from pathlib import Path
from typing import Literal, Union

import gcsfs
import geopandas as gpd
import pandas as pd
from shared_utils import catalog_utils

fs = gcsfs.GCSFileSystem()
SCHED_GCS = "gs://calitp-analytics-data/data-analyses/gtfs_schedule/"
PUBLIC_BUCKET = "gs://calitp-publish-data-analysis/"
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")


def write_to_public_gcs(
    original_filename_object: Union[str, Path],
    public_filename_object: Union[str, Path],
    public_bucket: str = PUBLIC_BUCKET,
) -> str:
    """
    Find the GCS object we want to write from our
    private GCS bucket and publish it to the public GCS.
    """
    local_filename = Path(original_filename_object).name

    # Download object from GCS to local
    fs.get(original_filename_object, local_filename)

    # Upload to GCS
    fs.put(
        local_filename,
        f"{public_bucket}{public_filename_object}",
    )

    print(f"Uploaded {public_filename_object}")
    os.remove(local_filename)

    return


def if_exists_then_delete(filepath: str):
    """
    Check if file exists in GCS and delete.
    For partitioned parquets, which are saved as folders, we need
    to use recursive=True.
    """
    if fs.exists(filepath):
        if fs.isdir(filepath):
            fs.rm(filepath, recursive=True)
        else:
            fs.rm(filepath)

    return


def exclude_private_datasets(
    df: pd.DataFrame,
    col: str = "schedule_gtfs_dataset_key",
    public_gtfs_dataset_keys: list = [],
) -> pd.DataFrame:
    """
    Filter out private datasets.
    """
    return df[df[col].isin(public_gtfs_dataset_keys)].reset_index(drop=True)


def subset_table_from_previous_date(
    gcs_bucket: str,
    filename: Union[str, Path],
    operator_and_dates_dict: dict,
    date: str,
    crosswalk_col: str = "schedule_gtfs_dataset_key",
    data_type: Literal["df", "gdf"] = "df",
) -> pd.DataFrame:
    CROSSWALK_FILE = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk

    crosswalk = pd.read_parquet(f"{SCHED_GCS}{CROSSWALK_FILE}_{date}.parquet", columns=["name", crosswalk_col])

    subset_keys = crosswalk[crosswalk.name.isin(operator_and_dates_dict[date])][crosswalk_col].unique()

    if data_type == "df":
        past_df = pd.read_parquet(
            f"{gcs_bucket}{filename}_{date}.parquet", filters=[[(crosswalk_col, "in", subset_keys)]]
        )
    else:
        past_df = gpd.read_parquet(
            f"{gcs_bucket}{filename}_{date}.parquet", filters=[[(crosswalk_col, "in", subset_keys)]]
        )

    return past_df
