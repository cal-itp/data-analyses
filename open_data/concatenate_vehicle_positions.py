"""
Concatenate batched vehicle positions 
and filter to one day.

Since RT is stored in UTC time, we download 2 days for 
vehicle positions, but need to grab grab 1 full day from that.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import gcsfs
import geopandas as gpd
import pandas as pd
import shapely
import sys

from dask import delayed
from loguru import logger

from shared_utils import dask_utils, schedule_rt_utils, utils
from update_vars import SEGMENT_GCS, analysis_date

fs = gcsfs.GCSFileSystem()

def concat_batches(analysis_date: str) -> dd.DataFrame:
    """
    Append individual operator vehicle position parquets together
    and cache a partitioned parquet
    """

    fs_list = fs.ls(f"{SEGMENT_GCS}")

    vp_files = [i for i in fs_list if "vp_raw" in i 
                and f"{analysis_date}_batch" in i]
    
    delayed_dfs = [delayed(pd.read_parquet)(f"gs://{f}") 
                   for f in vp_files]
    
    ddf = dd.from_delayed(delayed_dfs)
    
    ddf = schedule_rt_utils.localize_timestamp_col(
        ddf, ["location_timestamp"])
        
    ddf = ddf.assign(
        hour = dd.to_datetime(ddf.location_timestamp_local).dt.hour,
    ).rename(columns = {"gtfs_dataset_name": "_gtfs_dataset_name"})
    
    return ddf


def vp_into_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Change vehicle positions, which comes as df, into gdf.
    """
    # Drop Nones or else shapely will error
    df2 = df[df.location.notna()].reset_index(drop=True)
    
    geom = [shapely.wkt.loads(x) for x in df2.location]

    gdf = gpd.GeoDataFrame(
        df2, geometry=geom, 
        crs="EPSG:4326").drop(columns="location")
        
    return gdf


def remove_batched_parquets(analysis_date: str):
    """
    Remove the batches of parquet downloads. 
    These have file name pattern of *_batch*.
    """
    fs_list = fs.ls(f"{SEGMENT_GCS}")
    
    vp_files = [
        i for i in fs_list if "vp_raw" in i 
        and f"{analysis_date}_batch" in i
    ]
    
    concat_file = [i for i in fs_list if 
                   f"{analysis_date}_concat" in i 
    ]
    
    for f in vp_files:
        fs.rm(f)
    
    for f in concat_file:
        fs.rm(f, recursive=True)
       
    
if __name__ == "__main__":

    LOG_FILE = "./logs/download_vp_v2.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
        
    # Concatenate all the batches
    concatenated_vp_df = concat_batches(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"concat and filter batched data: {time1 - start}")
    
    concatenated_vp_df.to_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}_concat", 
        partition_on = "gtfs_dataset_key")
    
    time2 = datetime.datetime.now()
    logger.info(f"export concatenated vp: {time2 - time1}")
    
    # Import concatenated tabular vp and make it a gdf
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}_concat/"
    ).reset_index(drop=True)
    
    vp_gdf = vp_into_gdf(vp)

    utils.geoparquet_gcs_export(
        vp_gdf,
        SEGMENT_GCS,
        f"vp_{analysis_date}"
    )
    
    remove_batched_parquets(analysis_date)
    logger.info(f"remove batched parquets")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
    
    