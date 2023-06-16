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
from segment_speed_utils import helpers
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


def filter_to_analysis_date(
    df: pd.DataFrame,
    analysis_date: str
) -> pd.DataFrame:
    """
    Parse the location_timestamp to grab date
    and only keep rows that are for analysis_date
    """    
    df = schedule_rt_utils.localize_timestamp_col(
        df, "location_timestamp")
        
    df = df.assign(
        activity_date = pd.to_datetime(df.location_timestamp_local).dt.date,
        hour = pd.to_datetime(df.location_timestamp_local).dt.hour,
    )
    
    df2 = (df[df.activity_date == pd.to_datetime(analysis_date).date()]
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
    

def concat_batches_for_two_dates(
    analysis_date: str, 
    one_day_after: str
):
    """
    Concatenate across the batches for the 2 dates and export 
    as partitioned parquet.
    Partition on gtfs_dataset_key because dask.delay will import
    each operator later.
    """
    # Concatenate all the batched data for the 2 downloaded dates
    part1 = concat_batches(analysis_date)
    part1.to_parquet(f"{SEGMENT_GCS}vp_raw_{analysis_date}", 
                     partition_on = "gtfs_dataset_key")
    
    part2 = concat_batches(one_day_after)
    part2.to_parquet(f"{SEGMENT_GCS}vp_raw_{one_day_after}", 
                     partition_on = "gtfs_dataset_key")
    
    logger.info(f"concatenated all batched data")

    
def filter_by_operator_to_activity_date(
    analysis_date: str, 
    one_day_after: str
):  
    """
    Use dask delayed to go through all operators.
    For each operator, filter down to the activity date within 
    each downloaded date (service_date).
    Concatenate the 2 dates into one and save it as our actual analysis_date.
    """
    RT_OPERATORS = pd.read_parquet(
        f"{SEGMENT_GCS}vp_raw_{analysis_date}",
        columns = ["gtfs_dataset_key"]
    ).drop_duplicates().gtfs_dataset_key.unique().tolist()
        
    results = []
    
    for rt_dataset_key in RT_OPERATORS:
        part1 = delayed(pd.read_parquet)(
            f"{SEGMENT_GCS}vp_raw_{analysis_date}", 
            filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]]
        )
        part2 = delayed(pd.read_parquet)(
            f"{SEGMENT_GCS}vp_raw_{one_day_after}",
            filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]]
        )

        part1_filtered = delayed(filter_to_analysis_date)(part1, analysis_date)
        part2_filtered = delayed(filter_to_analysis_date)(part2, analysis_date)
        
        results.append(part1_filtered)
        results.append(part2_filtered)
    
        print(f"finished: {rt_dataset_key}")

    results_df = dd.from_delayed(results)     
    
    results_df = results_df.astype({"activity_date": "datetime64[ns]"})
    
    results_df.to_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}_concat", 
        overwrite = True
    )    
       
    
if __name__ == "__main__":

    LOG_FILE = "./logs/download_vp_v2.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    one_day_after = helpers.find_day_after(analysis_date)
    
    # Concatenate all the batches for the two dates
    concat_batches_for_two_dates(analysis_date, one_day_after)
    
    time1 = datetime.datetime.now()
    logger.info(f"concat and filter for 2 dates downloaded: {time1 - start}")
    
    # For each operator, filter it to the activity date we want
    # and concatenate and export
    filter_by_operator_to_activity_date(analysis_date, one_day_after)
    
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
    
    