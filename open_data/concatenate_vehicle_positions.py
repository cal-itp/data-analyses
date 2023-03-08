"""
Concatenate batched vehicle positions 
and filter to one day.

Since RT is stored in UTC time, we download 2 days for 
vehicle positions, but need to grab grab 1 full day from that.
"""
import dask.dataframe as dd
import datetime
import gcsfs
import geopandas as gpd
import pandas as pd
import shapely
import sys

from loguru import logger

from shared_utils import utils
from segment_speed_utils import helpers, segment_calcs
from update_vars import SEGMENT_GCS, analysis_date

fs = gcsfs.GCSFileSystem()

def concat_batches(analysis_date: str) -> pd.DataFrame:
    """
    Append individual operator vehicle position parquets together
    and cache a single vehicle positions parquet
    """

    fs_list = fs.ls(f"{SEGMENT_GCS}")

    vp_files = [i for i in fs_list if "vp_raw" in i 
                and f"{analysis_date}_batch" in i]
    
    df = pd.DataFrame()
    
    for f in vp_files:
        batch_df = pd.read_parquet(f"gs://{f}")
        df = pd.concat([df, batch_df], axis=0)
    
    return df


def vp_into_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Change vehicle positions, which comes as df, into gdf.
    """
    # Drop Nones or else shapely will error
    df = df[df.location.notna()].reset_index(drop=True)
    
    geom = [shapely.wkt.loads(x) for x in df.location]

    gdf = gpd.GeoDataFrame(
        df, geometry=geom, 
        crs="EPSG:4326").drop(columns="location")
    
    return gdf


def filter_to_analysis_date(
    df: dd.DataFrame,
    analysis_date: str
) -> dd.DataFrame:
    """
    Parse the location_timestamp to grab date
    and only keep rows that are for analysis_date
    """
    if isinstance(df, pd.DataFrame):
        df = dd.from_pandas(df, npartitions=3)
    
    df = segment_calcs.localize_vp_timestamp(
        df, "location_timestamp")
    
    df = df.assign(
        date = dd.to_datetime(df.location_timestamp_local).dt.date,
    )
    
    df2 = (df[df.date == pd.to_datetime(analysis_date)]
           .reset_index(drop=True)
           .drop(columns = "date")
          )
    
    return df2


def remove_batched_parquets(analysis_date: str):
    """
    Remove the batches of parquet downloads. 
    These have file name pattern of *_batch*.
    """
    one_day_after = helpers.find_day_after(analysis_date)

    fs_list = fs.ls(f"{SEGMENT_GCS}")
    
    vp_files = [
        i for i in fs_list if "vp_raw" in i 
        and ((f"{analysis_date}_batch" in i) or 
        (f"{one_day_after}_batch" in i))
    ]
    
    for f in vp_files:
        fs.rm(f)
    

def concat_and_filter_vp(analysis_date: str) -> pd.DataFrame: 
    """
    Since RT is UTC and that spans partly the day we want
    and we need to grab the day after,
    we will actually have 3 days worth of data from 2 days downloaded
    
    Concatenate the batched data we downloaded, save it.
    Then, filter to just the analysis_date we're interested in
    """
    one_day_after = helpers.find_day_after(analysis_date)
    
    # Concatenate all the batched data for the 2 downloaded dates
    part1 = concat_batches(analysis_date)
    part1.to_parquet(f"{SEGMENT_GCS}vp_raw_{analysis_date}.parquet")
    
    part2 = concat_batches(one_day_after)
    part2.to_parquet(f"{SEGMENT_GCS}vp_raw_{one_day_after}.parquet")
    
    logger.info(f"concatenated all batched data")

    # Filter down to analysis_date 
    part1_filtered = filter_to_analysis_date(part1, analysis_date)
    part2_filtered = filter_to_analysis_date(part2, analysis_date)

    
    # Concatenate the 2 dates downloaded that are filtered
    # appropriately to the analysis_date of interest
    all_vp = dd.multi.concat(
        [part1_filtered, part2_filtered], 
        axis=0).compute()
    
    return all_vp
    
    
if __name__ == "__main__":

    LOG_FILE = "../logs/download_vp_v2.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    all_vp = concat_and_filter_vp(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"concat and filter for 2 dates downloaded: {time1 - start}")
        
    gdf = vp_into_gdf(all_vp)
    
    time2 = datetime.datetime.now()
    logger.info(f"turn df into gdf: {time2 - time1}")

    utils.geoparquet_gcs_export(
        gdf, 
        SEGMENT_GCS,
        f"vp_{analysis_date}"
    )
    
    logger.info(f"export concatenated vp")
    
    remove_batched_parquets(analysis_date)
    logger.info(f"remove batched parquets")
    