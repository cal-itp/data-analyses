import os
from pathlib import Path
from typing import Union

import dask_geopandas as dg
import geopandas as gpd
from calitp_data_analysis import get_fs, utils

fs = get_fs()


def parse_file_directory(file_name: str) -> str:
    """
    Grab the directory of the filename.
    For GCS bucket, we do not want '.' as the parent
    directory, we want to parse and put together the
    GCS filepath correctly.
    """
    if str(Path(file_name).parent) != ".":
        return str(Path(file_name).parent)
    else:
        return ""


def geoparquet_gcs_export(gdf: Union[gpd.GeoDataFrame, dg.GeoDataFrame], gcs_file_path: str, file_name: str, **kwargs):
    """
    Save geodataframe as parquet locally,
    then move to GCS bucket and delete local file.

    gdf: geopandas.GeoDataFrame
    gcs_file_path: str
                    Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    file_name: str
                Filename, with or without .parquet.
    """
    # Parse out file_name into stem (file_name_sanitized)
    # and parent (file_directory_sanitized)
    file_name_sanitized = Path(utils.sanitize_file_path(file_name))
    file_directory_sanitized = parse_file_directory(file_name)

    # Make sure GCS path includes the directory we want the file to go to
    expanded_gcs = f"{Path(gcs_file_path).joinpath(file_directory_sanitized)}/"
    expanded_gcs = str(expanded_gcs).replace("gs:/", "gs://")

    if isinstance(gdf, dg.GeoDataFrame):
        gdf.to_parquet(f"{expanded_gcs}{file_name_sanitized}", overwrite=True, **kwargs)

    else:
        gdf.to_parquet(f"{file_name_sanitized}.parquet", **kwargs)
        fs.put(
            f"{file_name_sanitized}.parquet",
            f"{str(expanded_gcs)}{file_name_sanitized}.parquet",
        )
        os.remove(f"{file_name_sanitized}.parquet", **kwargs)
