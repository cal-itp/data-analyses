"""
Do a check of the operators to run and return a list.

Setting a list ahead of time vs running the tbl.gtfs_schedule.agency query
Want to be explicit in what operators should be able to run, ones we
expect not to run, and ones we expect should run and are erroring.

Return a list of operators that have cached files, and 
thus should be able to have their HQTA corridors compiled.
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import json
import pandas as pd
import sys

from loguru import logger
from siuba import *

from calitp.tables import tbls
from shared_utils import rt_utils
from update_vars import (analysis_date, CACHED_VIEWS_EXPORT_PATH, 
                        VALID_OPERATORS_FILE)

def get_list_of_cached_itp_ids(analysis_date: str, 
                               all_itp_ids: list = None) -> list:
    """
    ALL_ITP_IDS: list. 
                Allow passing an alternate list of IDs in. 
                If list is not specified, then run a fresh query.    
    """
    if all_itp_ids is None:
        all_itp_ids = (tbls.gtfs_schedule.agency()
                   >> distinct(_.calitp_itp_id)
                   >> filter(_.calitp_itp_id != 200, 
                             # Amtrak is always filtered out
                             _.calitp_itp_id != 13)
                   >> collect()
        ).calitp_itp_id.tolist()
    
    ITP_IDS_WITH_CACHED_FILES = []

    for itp_id in all_itp_ids:
        response1 = rt_utils.check_cached(
            f"routelines_{itp_id}_{analysis_date}.parquet", 
            subfolder="cached_views/")
        response2 = rt_utils.check_cached(
            f"trips_{itp_id}_{analysis_date}.parquet", subfolder="cached_views/")
        response3 = rt_utils.check_cached(
            f"st_{itp_id}_{analysis_date}.parquet", subfolder="cached_views/")
        response4 = rt_utils.check_cached(
            f"stops_{itp_id}_{analysis_date}.parquet", subfolder="cached_views/")
        
        all_responses = [response1, response2, response3, response4]
        if all(r is not None for r in all_responses):
            ITP_IDS_WITH_CACHED_FILES.append(itp_id)
    
    return sorted(ITP_IDS_WITH_CACHED_FILES)


def list_to_json(my_list: list, file: str):
    """
    Turn list to json
    """
    MY_DICT = {}
    MY_DICT["VALID_ITP_IDS"] = my_list
    
    with open(f"./{file.replace('.json', '')}.json", "w") as f:
        json.dump(MY_DICT, f)
        
    
def itp_ids_from_json(file: str = VALID_OPERATORS_FILE) -> list:
    # First, open the JSON with all the operators
    with open(f"./{file}") as f:
        data = json.load(f)
    
    return data["VALID_ITP_IDS"]


def check_for_completeness(
    export_path: str = CACHED_VIEWS_EXPORT_PATH, 
    all_itp_ids: list = [],
    analysis_date: str = analysis_date,
    valid_operators_file: str = VALID_OPERATORS_FILE) -> list:
    """
    Go through and check that the files are non-empty for all 4.
    
    Since stop_times is only downloaded if the first 3 are non-empty,
    just check for presence of stop_times file.
    
    Returns a potentially smaller list of operators that 
    we expect to run through rest of the HQTA workflow
    """
    IDS_WITH_FULL_INFO = [] 
    
    for itp_id in all_itp_ids:  
        stop_times = dd.read_parquet(
            f"{export_path}st_{itp_id}_{analysis_date}.parquet")

        if len(stop_times.index) > 0:  
            IDS_WITH_FULL_INFO.append(itp_id)
    
    return IDS_WITH_FULL_INFO

    
if __name__=="__main__":
    
    logger.add("./logs/operators_for_hqta.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
    # These are all the IDs that have some cached files in GCS
    ITP_IDS = get_list_of_cached_itp_ids(analysis_date)
    
    time1 = dt.datetime.now()
    logger.info(f"get list of cached ITP IDs: {time1-start}")
    
    # Now check whether an operator has a complete set of files (len > 0)
    IDS_WITH_FULL_INFO = check_for_completeness(
        CACHED_VIEWS_EXPORT_PATH, ITP_IDS, analysis_date, VALID_OPERATORS_FILE)
    
    # Save that list to json, should be smaller than all operators with cached
    list_to_json(IDS_WITH_FULL_INFO, VALID_OPERATORS_FILE)
    
    time2 = dt.datetime.now()
    logger.info(f"check files for completeness, save as json: {time2-time1}")

    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

        
    
