import os
from pathlib import Path
from typing import Union

import gcsfs
import pandas as pd

fs = gcsfs.GCSFileSystem()
PUBLIC_BUCKET = "gs://calitp-publish-data-analysis/"


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
