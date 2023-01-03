"""
Concatenate vehicle positions for operators on a single day.
Set linear reference for vehicle positions along shape_id.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import pandas as pd
import gcsfs
import geopandas as gpd

from dask import delayed, compute
from dask.delayed import Delayed # use this for type hint

from shared_utils import geography_utils, utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
analysis_date = "2022-10-12"


def get_scheduled_trips(analysis_date: str) -> dd.DataFrame:
    """
    Get scheduled trips info (all operators) for single day, 
    and keep subset of columns.
    """
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    keep_cols = ["calitp_itp_id", "calitp_url_number", 
                 "trip_id", "shape_id",
                 "route_id", "direction_id"
                ] 
    trips = trips[keep_cols]
    
    return trips


def get_routelines(
    analysis_date: str, buffer_size: int = 50
) -> dg.GeoDataFrame: 
    """
    Import routelines (shape_ids) and add route_length and buffer by 
    some specified size (50 m to start)
    """
    routelines = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet"
    ).to_crs(geography_utils.CA_NAD83Albers)
                 
    routelines = routelines.assign(
        route_length = routelines.geometry.length,
        shape_geometry_buffered = routelines.geometry.buffer(buffer_size)
    )
    
    return routelines
        

if __name__ == "__main__":
    
    # Append individual operator vehicle position parquets together
    # and cache a single vehicle positions parquet
    fs = gcsfs.GCSFileSystem()
    fs_list = fs.ls(f"{DASK_TEST}vp/")

    vp_files = [i for i in fs_list if "vp_" in i and analysis_date in i]

    full_df = dd.read_parquet(f"gs://{vp_files[0]}")

    for f in vp_files[1:]:    
        df = dd.read_parquet(f"gs://{f}")
        full_df = dd.multi.concat([full_df, df], axis=0)


    full_df = (full_df.drop_duplicates()
               .reset_index(drop=True)
               .compute()
              )

    gdf = geography_utils.create_point_geometry(
        full_df, 
        longitude_col = "vehicle_longitude",
        latitude_col = "vehicle_latitude",
    ).drop(columns = ["vehicle_longitude", "vehicle_latitude"])
         
                  
    utils.geoparquet_gcs_export(
        gdf,
        DASK_TEST,
        f"vp_{analysis_date}"
    )
