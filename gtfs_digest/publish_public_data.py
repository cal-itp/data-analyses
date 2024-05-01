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
from update_vars import GTFS_DATA_DICT 
  
PUBLIC_GCS = GTFS_DATA_DICT.gcs_paths.PUBLIC_GCS
    
def grab_filepaths(
    table_section: Literal["digest_tables", "schedule_tables"], 
    file_keys: list) -> list:
    """
    https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/gtfs_analytics_data.yml
    
    table_section corresponds to "schedule_tables", "digest_tables", 
    "speeds_tables", etc
    """
    
    GCS = GTFS_DATA_DICT[table_section].dir
    
    filepaths = [GTFS_DATA_DICT[table_section][f] for f in file_keys]
    
    return [f"{GCS}{f}.parquet" for f in filepaths]
    

def export_parquet_as_csv_or_geojson(
    filename: str,
    filetype: Literal["df", "gdf"],
):
    """
    For parquets, we want to export as csv.
    For geoparquets, we want to export as geojson.
    """
    if filetype=="df":
        df = pd.read_parquet(filename)
        df.to_csv(
            f"{PUBLIC_GCS}gtfs_digest/"
            f"{Path(filename).stem}.csv", index=False
        )
        
        
    elif filetype=="gdf":
        df = gpd.read_parquet(filename)
        utils.geojson_gcs_export(
            df,
            f"{PUBLIC_GCS}gtfs_digest/",
            Path(filename).stem,
            geojson_type = "geojson"
        )
        
    return
        
        

if __name__ == "__main__":
    
    digest_gdf_keys = [
        "operator_routes_map"
    ]
    
    digest_df_keys = [
        "route_schedule_vp", 
        "operator_profiles",  
        "operator_sched_rt"
    ]  
    
    df_filepaths = (
        grab_filepaths("digest_tables", digest_df_keys) + 
        grab_filepaths("schedule_tables", ["monthly_scheduled_service"])
    )
    
    gdf_filepaths = grab_filepaths("digest_tables", digest_gdf_keys)
    
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
