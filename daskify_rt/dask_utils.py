"""
Utility functions for wrangling Dask data processing steps.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import gcsfs

from dask import compute
from dask.delayed import Delayed # type hint
from typing import List, Literal

from shared_utils import utils

fs = gcsfs.GCSFileSystem()                    

def concat_and_export(
    filename: str, 
    filetype: Literal["df", "gdf"] = "df"):
    """
    Read in a folder of partitioned parquets and export as 1 parquet.
    In the filename, include the full GCS path.
    
    Clarify this more. Need to remove directory, but not allow
    gs://bucket_name/folder/my_file.parquet//, 
    yet allow gs://bucket_name/folder/my_file.parquet/, 
    which contains multipart parquets
    """
    filename_sanitized = f"{filename.replace('.parquet', '')}"
    # Don't want to add extra / at the end of the directory path for
    # partitioned parquets
    if filename_sanitized[-1] == "/":
        filename_sanitized = filename_sanitized[:-1]
        
    filename_only = filename_sanitized.split('/')[-1]

    if filetype == "df":
        ddf = dd.read_parquet(filename)
        ddf.compute().to_parquet(f"{filename_sanitized}.parquet")
        
    elif filetype == "gdf":
        gddf = dg.read_parquet(filename)
        gdf = gddf.compute()
        utils.geoparquet_gcs_export(
            gdf, 
            filename.replace(filename_only, ''),
            filename_only
        )
        
    # Remove the folder version, not the single parquet
    fs.rm(f"{filename}/", recursive=True)
    
    
def compute_and_export(
    results: List[Delayed], gcs_folder: str, 
    file_name: str, 
    export_single_parquet: bool = True
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
    
    # If we're saving partitioned parquets, make sure that the 
    # file path doesn't end with .parquet, because then the folder 
    # includes .parquet, and within the folder, there's more part*.parquet
    if not export_single_parquet:
        ddf2.to_parquet(
            f"{gcs_folder}{file_name.replace('.parquet', '')}"
        )
    
    # If we want to export a single parquet, which we usually want
    # for our final results
    elif export_single_parquet:
        if isinstance(ddf2, dd.DataFrame):
            concat_and_export(f"{gcs_folder}{file_name}", filetype = "df")

        elif isinstance(ddf2, dg.GeoDataFrame):
            concat_and_export(f"{gcs_folder}{file_name}", filetype = "gdf")

    