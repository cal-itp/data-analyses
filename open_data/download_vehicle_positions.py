"""
Download vehicle positions for a day.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)

import datetime
import gcsfs
import geopandas as gpd
import pandas as pd
import shapely
import sys

from calitp_data_analysis.tables import tbls
from loguru import logger
from siuba import *

from shared_utils import utils, gtfs_utils_v2
from update_vars import SEGMENT_GCS, analysis_date

fs = gcsfs.GCSFileSystem()

def determine_batches(rt_names: list) -> dict:
    #https://stackoverflow.com/questions/4843158/how-to-check-if-a-string-is-a-substring-of-items-in-a-list-of-strings
    large_operator_names = [
        "LA Metro Bus",
        "LA Metro Rail",
        "AC Transit", 
        "Muni"
    ]
    
    bay_area_names = [
        "Bay Area 511"
    ]

    # If any of the large operator name substring is 
    # found in our list of names, grab those
    # be flexible bc "Vehicle Positions" and "VehiclePositions" present
    matching = [i for i in rt_names 
                if any(name in i for name in large_operator_names)]
    
    remaining_bay_area = [i for i in rt_names 
                          if any(name in i for name in bay_area_names) and 
                          i not in matching
                         ]
    remaining = [i for i in rt_names if 
                 i not in matching and i not in remaining_bay_area]
    
    # Batch large operators together and run remaining in 2nd query
    batch_dict = {}
    
    batch_dict[0] = matching
    batch_dict[1] = remaining_bay_area
    batch_dict[2] = remaining
    
    return batch_dict


def download_vehicle_positions(
    date: str,
    operator_names: list
) -> pd.DataFrame:    
    
    df = (tbls.mart_gtfs.fct_vehicle_locations()
          >> filter(_.dt == date)
          >> filter(_._gtfs_dataset_name.isin(operator_names))
          >> select(_.gtfs_dataset_key, _._gtfs_dataset_name,
                    _.trip_id,
                    _.location_timestamp,
                    _.location)
              >> collect()
         )
    
    # query_sql, parsing by the hour timestamp BQ column confusing
    #https://www.yuichiotsuka.com/bigquery-timestamp-datetime/
    
    return df


def concat_batches(analysis_date: str) -> gpd.GeoDataFrame:
    # Append individual operator vehicle position parquets together
    # and cache a single vehicle positions parquet
    fs_list = fs.ls(f"{SEGMENT_GCS}")

    vp_files = [i for i in fs_list if "vp_raw" in i 
                and f"{analysis_date}_batch" in i]
    
    df = pd.DataFrame()
    
    for f in vp_files:
        batch_df = pd.read_parquet(f"gs://{f}")
        df = pd.concat([df, batch_df], axis=0)
        
    # Drop Nones or else shapely will error
    df = df[df.location.notna()].reset_index(drop=True)
    
    geom = [shapely.wkt.loads(x) for x in df.location]

    gdf = gpd.GeoDataFrame(
        df, geometry=geom, 
        crs="EPSG:4326").drop(columns="location")
    
    return gdf


def remove_batched_parquets(analysis_date):
    fs_list = fs.ls(f"{SEGMENT_GCS}")

    vp_files = [i for i in fs_list if "vp_raw" in i 
                and f"{analysis_date}_batch" in i]
    
    for f in vp_files:
        fs.rm(f)


if __name__ == "__main__":
    from dask.distributed import Client
    
    client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("../logs/download_vp_v2.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    # Get rt_datasets that are available for that day
    rt_datasets = gtfs_utils_v2.get_transit_organizations_gtfs_dataset_keys(
        keep_cols=["key", "name", "type", "regional_feed_type"],
        custom_filtering={"type": ["vehicle_positions"]},
        get_df = True
    ) >> collect()
    
    
    # Exclude regional feed and precursors
    exclude = ["Bay Area 511 Regional VehiclePositions"]
    rt_datasets = rt_datasets[
        ~(rt_datasets.name.isin(exclude)) & 
        (rt_datasets.regional_feed_type != "Regional Precursor Feed")
    ].reset_index(drop=True)
    
    rt_dataset_names = rt_datasets.name.unique().tolist()

    batches = determine_batches(rt_dataset_names)
    
    for i, subset_operators in batches.items():
        time0 = datetime.datetime.now()

        logger.info(f"batch {i}: {subset_operators}")
        df = download_vehicle_positions(
            analysis_date, subset_operators)

        df.to_parquet(
            f"{SEGMENT_GCS}vp_raw_{analysis_date}_batch{i}.parquet")

        time1 = datetime.datetime.now()
        logger.info(f"exported batch {i} to GCS: {time1 - time0}")
    
    
    all_vp = concat_batches(analysis_date)
    logger.info(f"concatenated all batched data")
    
    utils.geoparquet_gcs_export(
        all_vp, 
        SEGMENT_GCS,
        f"vp_{analysis_date}"
    )
    
    logger.info(f"export concatenated vp")
    
    remove_batched_parquets(analysis_date)
    logger.info(f"remove batched parquets")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
        
    client.close()