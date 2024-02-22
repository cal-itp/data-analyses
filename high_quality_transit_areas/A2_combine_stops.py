"""
Combine rail, BRT, and ferry stops data.

Before running, check BRT stop maps in notebooks,
and filter out certain stop_ids.

Export combined rail_brt_ferry data into GCS.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

import A1_download_rail_ferry_brt_stops as rail_ferry_brt
from calitp_data_analysis import utils
from utilities import GCS_FILE_PATH, clip_to_ca
from update_vars import analysis_date, TEMP_GCS


metro_street_running = [
    '141012', '13805', '5397', '13803',
    '13804', '5396', '13802', '5395', '5410', '5411', '13817',
    '12304', '5408', '3821', '2603', '3153', '3124', '378', '65300039',
    '65300038', '15820', '13460', '4994', '1813', '2378', '5049',
    '4652', '2377', '4675', '5040', '65300042', '3674', '15713',
    '13561', '5378', '13560', '70500012', '5377', '15612',
    '12416', '11917', '12415', '8704'
]

van_ness_ids = [
    '8096', '8097', '18095', '18098', '8094', '8099', '18093', '8100',
    '18092', '18101', '18102', '18091', '18103', '8090', '18104', '18089',
    '18105', '18088'
]

new_muni_stops = [
    '17876', '17875', # Chinatown-Rose Pak 
    '17877', '17874', # Union Square/Market Street
    '17873', '17878', # Yerba Buena/Moscone
    '13156', '3156', # 4th & Brannan
]

BRT_STOPS_FILTER = {
    "LA Metro Bus Schedule": metro_street_running,
    "Bay Area 511 Muni Schedule": van_ness_ids
}


if __name__ == "__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")

    logger.info(f"A2_combine_stops Analysis Date: {analysis_date}")
    start = datetime.datetime.now()
    
    # Rail
    rail_ferry_brt.grab_rail_data(analysis_date)
    rail_stops = gpd.read_parquet(f"{TEMP_GCS}rail_stops.parquet")
    
    time1 = datetime.datetime.now()
    logger.info(f"grabbed rail: {time1-start}")

    # BRT
    rail_ferry_brt.grab_brt_data(analysis_date)
    brt_stops = gpd.read_parquet(f"{TEMP_GCS}brt_stops.parquet")
    brt_stops = rail_ferry_brt.additional_brt_filtering_out_stops(
        brt_stops, BRT_STOPS_FILTER)

    time2 = datetime.datetime.now()
    logger.info(f"grabbed brt: {time2-time1}")
    
    # Ferry
    rail_ferry_brt.grab_ferry_data(analysis_date)
    ferry_stops = gpd.read_parquet(f"{TEMP_GCS}ferry_stops.parquet")
    
    time3 = datetime.datetime.now()
    logger.info(f"grabbed ferry: {time3-time2}")
    
    # Concatenate datasets that need to be clipped to CA
    rail_brt = pd.concat([
        rail_stops,
        brt_stops
    ], axis=0, ignore_index= True)
    
    rail_brt = clip_to_ca(rail_brt)
    
    # Concatenate all together
    rail_brt_ferry = pd.concat([
        rail_brt,
        ferry_stops
    ], axis=0, ignore_index=True)
    
    # Export to GCS
    utils.geoparquet_gcs_export(
        rail_brt_ferry, 
        GCS_FILE_PATH, 
        'rail_brt_ferry'
    )
    
    end = datetime.datetime.now()
    logger.info(f"A2_combine_stops execution time: {end-start}")
    
    #client.close()