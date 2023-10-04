"""
Apply exponential decay function and sum
up by origin point.
Merge results back onto original full POI file.
"""
import dask.dataframe as dd
import datetime 
import geopandas as gpd
import gcsfs
import numpy as np
import os
import pandas as pd
import shutil
import sys

from dask import delayed, compute
from loguru import logger
from calitp_data_analysis import utils
from prep import GCS_FILE_PATH

fs = gcsfs.GCSFileSystem()

def aggregate_by_origin(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Sum up the decay-weighted opportunities by origin point.
    """
    access = (df.groupby("origin_poi_index")
              .decay_weighted_opps
              .sum()
              .reset_index()
             ).rename(columns = {"origin_poi_index": "poi_index"})
        
    return access


def decay_weighted_opportunities(
    df: pd.DataFrame, 
    distance_col: str,
    speed_mph: int = 10,
    time_cutoff_min: int = 60
):
    """
    Use assumptions for exponential decay.
    SPEED = 10 mph
    CUTOFF (time cutoff, minutes) = 60
    """
    if isinstance(df, pd.DataFrame):
        df = dd.from_pandas(df, npartitions=1)
        
    # Split the df up into ones where the origin intersects with itself (no decay)
    # from where the origin intersects with other points (some distance calculated)
    distance_zero_ddf = df[df.origin_poi_index == df.destination_poi_index]
    decay_ddf = df[df.origin_poi_index != df.destination_poi_index]
    
    # Use arrays to apply the exponential decay formula
    oppor_arr = decay_ddf.grid_code.to_dask_array()
    distance_arr = decay_ddf[distance_col].to_dask_array()
    
    decay_opps_series = (
        oppor_arr * np.exp(np.log(0.5) / 
        (time_cutoff_min * 60) * 
        (((60 * distance_arr * 0.000621371) / speed_mph) * 60))
    )
    
    decay_ddf["decay_weighted_opps"] = decay_opps_series

    # No decay for own opportunities (origin intersecting with itself)
    # Set the decay weighted opps to just be grid_code here
    distance_zero_ddf = distance_zero_ddf.assign(
        decay_weighted_opps = distance_zero_ddf.grid_code
    )
    
    # Concatenate two parts of df
    decay_results = dd.multi.concat(
        [decay_ddf, distance_zero_ddf], axis=0)

    return decay_results


def weighted_opps_by_region(region: str) -> dd.DataFrame:  
    """
    Apply the exponential decay function to 
    number of opportunities. Sum it up by origin.
    """
    ddf = dd.read_parquet(
        f"{GCS_FILE_PATH}sjoin_pairs_{region}/"
    ).repartition(partition_size="50MB")
    
    region_oppor = decay_weighted_opportunities(
        ddf,
        distance_col = "dist",
        speed_mph = 10,
        time_cutoff_min = 60,
    )
    
    access = aggregate_by_origin(region_oppor)
        
    return access


def finalize_and_export_results(results_file_name: str):
    """
    Read in the partitioned parquet of results, 
    and merge back with the full gdf of concatenated POIs.
    Our results file is bare bones, just poi_index and decay_weighted_opps.
    Merge back into the full gdf to get pointid, Point_ID, geometry, etc.
    """
    result_df = dd.read_parquet(f"./{results_file_name}/").compute()
    all_pois = gpd.read_parquet(f"{GCS_FILE_PATH}all_pois.parquet")

    final = pd.merge(
        all_pois,
        result_df,
        on = "poi_index",
        how = "left"
    )
    
    final = final.assign(
        decay_weighted_opps = final.decay_weighted_opps.fillna(0)
    )
    
    utils.geoparquet_gcs_export(
        final,
        GCS_FILE_PATH,
        results_file_name
    )    
    
    shutil.rmtree(f"./{results_file_name}/")

    
if __name__ == "__main__":
    
    LOG_FILE = "./logs/t3_decay.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
                
    REGIONS = ["CentralCal", "Mojave", "NorCal", "SoCal"]
    
    results = [delayed(weighted_opps_by_region)(region) for region in REGIONS]
    
    time1 = datetime.datetime.now()
    logger.info(f"get list of delayed: {time1-start}")
    
    results2 = [compute(i)[0] for i in results]
    
    time2 = datetime.datetime.now()
    logger.info(f"compute list of results: {time2-time1}")
    
    ddf = dd.multi.concat(results2, axis=0).reset_index(drop=True)
    
    RESULTS_EXPORT_FILE = "oppor_results"
    ddf.to_parquet(f"./{RESULTS_EXPORT_FILE}/", overwrite=True)
    
    time3 = datetime.datetime.now()
    logger.info(f"concatenate and export partitioned results: {time3-time2}")    
    
    finalize_and_export_results(RESULTS_EXPORT_FILE)
    
    time4 = datetime.datetime.now()
    logger.info(f"export final results as parquet: {time4-time3}")
    
    final = gpd.read_parquet(f"{GCS_FILE_PATH}oppor_results.parquet")
    
    # make zipped shapefile
    utils.make_zipped_shapefile(
        final,
        f"{RESULTS_EXPORT_FILE}.zip"
    )
    
    # Upload to GCS
    fs.put(
        f"{RESULTS_EXPORT_FILE}.zip"
        f"{GCS_FILE_PATH}{RESULTS_EXPORT_FILE}.zip"
    )
    
    # Remove local version
    os.remove(f"{RESULTS_EXPORT_FILE}.zip")
    
    end = datetime.datetime.now()
    logger.info(f"zip and upload zipped shapefile: {end-time4}")
    logger.info(f"execution time: {end-start}")