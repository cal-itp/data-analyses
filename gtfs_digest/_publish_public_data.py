"""
Export the tables we used in the notebook
to create GTFS Digest to our public bucket.
"""
import geopandas as gpd
import pandas as pd

from pathlib import Path
from typing import Literal

from calitp_data_analysis import utils
from shared_utils import publish_utils
from update_vars import GTFS_DATA_DICT, file_name
  
PUBLIC_GCS = GTFS_DATA_DICT.gcs_paths.PUBLIC_GCS

import google.auth
credentials, project = google.auth.default()

from functools import cache
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from calitp_data_analysis.gcs_pandas import GCSPandas

@cache
def gcs_pandas():
    return GCSPandas()
    
def grab_filepaths(
    table_section: Literal["gtfs_digest_rollup"], 
    file_keys: list,
    file_name: str) -> list:
    """
    https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/gtfs_analytics_data.yml
    
    table_section corresponds to "schedule_tables", "digest_tables", 
    "speeds_tables", etc
    """
    GCS = GTFS_DATA_DICT[table_section].dir
    
    file_paths = [GTFS_DATA_DICT[table_section][f] for f in file_keys]
    
    return [f"{GCS}processed/{f}_{file_name}.parquet" for f in file_paths]
    
def export_parquet_as_csv_or_geojson(
    filename: str,
    filetype: Literal["df", "gdf"],
):
    """
    For parquets, we want to export as csv.
    For geoparquets, we want to export as geojson.
    """
    if filetype=="df":
        df = gcs_pandas().read_parquet(filename)
        df.to_csv(
            f"{PUBLIC_GCS}gtfs_digest/"
            f"{Path(filename).stem}.csv", index=False
        )
        
        
    elif filetype=="gdf":
        df = gpd.read_parquet(filename, storage_options={"token": credentials.token},)
        utils.geojson_gcs_export(
            df,
            f"{PUBLIC_GCS}gtfs_digest/",
            Path(filename).stem,
            geojson_type = "geojson"
        )
        
        
if __name__ == "__main__":
    
    digest_gdf_keys = [
        "route_map"]
    
    digest_df_keys = [
        "schedule_rt_route_direction",
        "operator_summary",
        "hourly_day_type_summary",
    ] 
    
    df_filepaths = (
        grab_filepaths("gtfs_digest_rollup", digest_df_keys, file_name)
    )
    
    gdf_filepaths = grab_filepaths("gtfs_digest_rollup", digest_gdf_keys, file_name)
    
    # copy our private files to public GCS
    # for df ones, export as csv too
    # for gdf ones, export as geojson
    for f in df_filepaths + gdf_filepaths:
        publish_utils.write_to_public_gcs(
            f,
            f"gtfs_digest/{Path(f).name}",
            PUBLIC_GCS
        )
    
    for f in df_filepaths:
        export_parquet_as_csv_or_geojson(f, filetype="df")
        
    for f in gdf_filepaths:
        export_parquet_as_csv_or_geojson(f, filetype="gdf")
