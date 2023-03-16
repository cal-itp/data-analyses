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

from shared_utils import dask_utils, utils
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
    df2 = df[df.location.notna()].reset_index(drop=True)
    
    geom = [shapely.wkt.loads(x) for x in df2.location]

    gdf = gpd.GeoDataFrame(
        df2, geometry=geom, 
        crs="EPSG:4326").drop(columns="location")
        
    return gdf


def filter_to_analysis_date(
    df: pd.DataFrame,
    analysis_date: str
) -> pd.DataFrame:
    """
    Parse the location_timestamp to grab date
    and only keep rows that are for analysis_date
    """    
    df = segment_calcs.localize_vp_timestamp(
        df, "location_timestamp")
        
    df = df.assign(
        activity_date = pd.to_datetime(df.location_timestamp_local).dt.date,
        hour = pd.to_datetime(df.location_timestamp_local).dt.hour,
    )
    
    df2 = (df[df.activity_date == pd.to_datetime(analysis_date)]
           .reset_index(drop=True)
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
    
    concat_file = [i for i in fs_list if 
                   f"{analysis_date}_concat" in i 
    ]
    
    for f in vp_files:
        fs.rm(f)
    
    for f in concat_file:
        fs.rm(f, recursive=True)

        
def concat_and_filter_for_operator(
    analysis_date: str, 
    rt_dataset_key: str
) -> gpd.GeoDataFrame:
    # Filter down to analysis_date 
    part1 = pd.read_parquet(
        f"{SEGMENT_GCS}vp_raw_{analysis_date}.parquet", 
        filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]]
    )
    part2 = pd.read_parquet(
        f"{SEGMENT_GCS}vp_raw_{one_day_after}.parquet",
        filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]]
    )

    part1_filtered = filter_to_analysis_date(part1, analysis_date)
    part2_filtered = filter_to_analysis_date(part2, analysis_date)

    # Concatenate the 2 dates downloaded that are filtered
    # appropriately to the analysis_date of interest
    all_vp = pd.concat(
        [part1_filtered, part2_filtered], 
        axis=0
    ).reset_index(drop=True)
    
    gdf = vp_into_gdf(all_vp)
    
    return gdf

    
if __name__ == "__main__":

    LOG_FILE = "../logs/download_vp_v2.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    one_day_after = helpers.find_day_after(analysis_date)
    
    # Concatenate all the batched data for the 2 downloaded dates
    part1 = concat_batches(analysis_date)
    part1.to_parquet(f"{SEGMENT_GCS}vp_raw_{analysis_date}.parquet")
    
    part2 = concat_batches(one_day_after)
    part2.to_parquet(f"{SEGMENT_GCS}vp_raw_{one_day_after}.parquet")
    
    logger.info(f"concatenated all batched data")
    
    operators = pd.read_parquet(
        f"{SEGMENT_GCS}vp_raw_{analysis_date}.parquet",
        columns = ["gtfs_dataset_key"]
    ).drop_duplicates()
    
    RT_OPERATORS = operators.gtfs_dataset_key.tolist()
    
    results = []
    
    for rt_dataset_key in RT_OPERATORS:
        operator_df = delayed(concat_and_filter_for_operator)(
            analysis_date, rt_dataset_key) 
        
        results.append(operator_df)
        
        print(f"finished: {rt_dataset_key}")
       
        
    time1 = datetime.datetime.now()
    logger.info(f"concat and filter for 2 dates downloaded: {time1 - start}")
        
    dask_utils.compute_and_export(
        results,
        SEGMENT_GCS,
        f"vp_{analysis_date}_concat",
        export_single_parquet = False
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"export concatenated vp: {time2 - time1}")
    
    vp = dg.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}_concat/"
    ).compute()
    
    
    utils.geoparquet_gcs_export(
        vp,
        SEGMENT_GCS,
        f"vp_{analysis_date}"
    )
    
    remove_batched_parquets(analysis_date)
    logger.info(f"remove batched parquets")
    
    