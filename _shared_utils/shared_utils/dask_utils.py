"""
Utility functions for wrangling Dask data processing steps.
"""
from typing import List, Literal, Union

import dask.dataframe as dd
import dask_geopandas as dg
import gcsfs
import geopandas as gpd
import google.auth
import pandas as pd
from calitp_data_analysis import utils
from dask import compute, delayed
from dask.delayed import Delayed  # type hint
from shared_utils import time_helpers

credentials, project = google.auth.default()

fs = gcsfs.GCSFileSystem()


def concat_and_export(gcs_folder: str, file_name: str, filetype: Literal["df", "gdf"] = "df"):
    """
    Read in a folder of partitioned parquets and export as 1 parquet.
    In the filename, include the full GCS path.

    Clarify this more. Need to remove directory, but not allow
    gs://bucket_name/folder/my_file.parquet//,
    yet allow gs://bucket_name/folder/my_file.parquet/,
    which contains multipart parquets
    """
    filename_sanitized = f"{file_name.replace('.parquet', '')}"

    if not gcs_folder.startswith("gs://"):
        gcs_folder = f"gs://{gcs_folder}"

    if filetype == "df":
        print(f"Read in {gcs_folder}{file_name}")
        print(f"Save out {gcs_folder}{filename_sanitized}.parquet")

        ddf = dd.read_parquet(f"{gcs_folder}{file_name}")
        ddf.compute().to_parquet(f"{gcs_folder}{filename_sanitized}.parquet")

    elif filetype == "gdf":
        print(f"Read in {gcs_folder}{file_name}")
        print(f"Save out {gcs_folder}{filename_sanitized}.parquet")

        gddf = dg.read_parquet(f"{gcs_folder}{file_name}")
        gdf = gddf.compute()
        utils.geoparquet_gcs_export(gdf, gcs_folder, filename_sanitized)

    # Remove the folder version, not the single parquet
    fs.rm(f"{gcs_folder}{file_name}/", recursive=True)


def compute_and_export(
    results: List[Delayed],
    gcs_folder: str,
    file_name: str,
    export_single_parquet: bool = True,
):
    """
    Run compute() on list of dask.delayed results.
    Repartition to save it as partitioned parquets in GCS.
    Then, import those partitioned parquets and concatenate, and
    export as 1 single parquet.

    This is faster trying to run .compute() on the computed delayed object.
    """
    results2 = [compute(i)[0] for i in results]
    ddf = dd.multi.concat(results2, axis=0).reset_index(drop=True)
    ddf2 = ddf.repartition(partition_size="85MB")

    # Don't want to add extra '/' or '.parquet' at the end of the directory
    # path for partitioned parquets
    if file_name[-1] == "/":
        file_name = file_name[:-1]

    # Partitioned parquets must be saved out first, even if ultimate goal is to
    # save a single parquet
    # concat_and_export looks for a directory of partitioned parquets
    file_name_sanitized = file_name.replace(".parquet", "")
    ddf2.to_parquet(f"{gcs_folder}{file_name_sanitized}", overwrite=True)

    # If we want to export a single parquet, which we usually want
    # for our final results
    if export_single_parquet:
        if isinstance(ddf2, dd.DataFrame):
            concat_and_export(gcs_folder, file_name_sanitized, filetype="df")

        elif isinstance(ddf2, dg.GeoDataFrame):
            concat_and_export(gcs_folder, file_name_sanitized, filetype="gdf")


def concatenate_list_of_files(
    list_of_filepaths: list, file_type: Literal["df", "gdf"]
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Concatenate an imported list of filenames read in
    with pandas or geopandas. Use dask.delayed to loop through and
    assemble a concatenated pandas or geopandas dataframe.
    """
    if file_type == "df":
        dfs = [delayed(pd.read_parquet)(f) for f in list_of_filepaths]

    elif file_type == "gdf":
        dfs = [delayed(gpd.read_parquet)(f) for f in list_of_filepaths]

    results = [compute(i)[0] for i in dfs]
    full_df = pd.concat(results, axis=0).reset_index(drop=True)

    return full_df


def import_df_func(
    path: str,
    one_date: str,
    data_type: Literal["df", "gdf"] = "df",
    add_date: bool = False,
    add_year: bool = False,
    **kwargs,
):
    """
    Set up function with little modifications based on
    the dask docs. Modifications are that we want to read in
    pandas or geopandas df for a single date.

    https://docs.dask.org/en/latest/generated/dask.dataframe.from_map.html
    https://blog.dask.org/2023/04/12/from-map
    """
    if data_type == "gdf":
        df = gpd.read_parquet(
            f"{path}_{one_date}.parquet", **kwargs, storage_options={"token": credentials.token}
        ).drop_duplicates()

    else:
        df = pd.read_parquet(
            f"{path}_{one_date}.parquet",
            **kwargs,
        ).drop_duplicates()

    if add_date:
        df = time_helpers.add_service_date(df, one_date)
    if add_year:
        df = df.assign(year=int(one_date))
    return df


def import_ddf_func(path, date_list, data_type, **kwargs):
    """
    Equivalent to improt_df_func, except uses dask to read in the dataframe
    instead of pandas.
    Concatenates the various dates.
    """
    if data_type == "df":
        ddf = dd.multi.concat(
            [
                dd.read_parquet(f"{path}_{one_date}.parquet", **kwargs).assign(service_date=one_date)
                for one_date in date_list
            ],
            axis=0,
            ignore_index=True,
        )

    elif data_type == "gdf":
        ddf = dd.multi.concat(
            [
                dg.read_parquet(f"{path}_{one_date}.parquet", **kwargs).assign(service_date=one_date)
                for one_date in date_list
            ],
            axis=0,
            ignore_index=True,
        )

    return ddf


def get_ddf(paths, date_list, data_type, get_pandas: bool = False, **kwargs):
    """
    Set up function with little modifications based on
    the dask docs. Modifications are that we want to read in
    a list of dates.

    https://docs.dask.org/en/latest/generated/dask.dataframe.from_map.html
    https://blog.dask.org/2023/04/12/from-map
    """
    ddf = dd.from_map(import_df_func, paths, date_list, data_type=data_type, **kwargs).drop_duplicates()

    if get_pandas:
        ddf = ddf.compute()

    return ddf
