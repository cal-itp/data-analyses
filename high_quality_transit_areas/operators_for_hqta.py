"""
Do a check of the operators to run and return a list.

Want to be explicit in what operators should be able to run, ones we
expect not to run, and ones we expect should run and are erroring.

Return a dict of operator names to operator feed_keys that have cached files, and 
thus should be able to have their HQTA corridors compiled.
"""
import dask.dataframe as dd
import datetime as dt
import json
import pandas as pd
import sys

from loguru import logger

from shared_utils import rt_utils, gtfs_utils_v2
from update_vars import (analysis_date, CACHED_VIEWS_EXPORT_PATH, TEMP_GCS,
                        VALID_OPERATORS_FILE)


def scheduled_operators_for_hqta(analysis_date: str):
    """
    From schedule daily feeds to organization names table, 
    exclude Bay Area 511 combined regional feed and Amtrak.
    All other feed_keys are ones we would use.
    
    Cache this df into GCS, and clear it once it goes into the json. 
    """
    path = rt_utils.check_cached(
        filename = "operators_for_hqta.parquet", 
        subfolder="temp/"
    )    

    if path:
        operators_to_include = pd.read_parquet(
            f"{TEMP_GCS}operators_for_hqta.parquet")
    else:
    
        all_operators = gtfs_utils_v2.schedule_daily_feed_to_organization(
            selected_date = analysis_date,
            keep_cols = None,
            get_df = True
        )

        exclude = ["Bay Area 511 Regional Schedule", #we favor regional subfeeds
                   "Amtrak Schedule"]

        keep_cols = ["feed_key", "name"]

        operators_to_include = all_operators[
            ~(all_operators.name.isin(exclude)) &
            (all_operators.regional_feed_type != "Regional Precursor Feed")
        ][keep_cols]
    
        # There shouldn't be any duplicates by name, since we got rid 
        # of precursor feeds. But, just in case, don't allow dup names.
        operators_to_include = (operators_to_include
                                .drop_duplicates(subset="name")
                                .reset_index(drop=True)
                               )

        operators_to_include.to_parquet(f"{TEMP_GCS}operators_for_hqta.parquet")

    return operators_to_include


def name_feed_key_dict_to_json(operators_df: pd.DataFrame,
                               file: str):
    # Put name as the key, in case feed_key for operator changes
    # over time, we still have a way to catalog this
    name_feed_key_dict = dict(zip(operators_df.name, operators_df.feed_key))
    
    MY_DICT = {}
    MY_DICT["VALID_FEED_KEYS"] = name_feed_key_dict
    
    with open(f"./{file.replace('.json', '')}.json", "w") as f:
        json.dump(MY_DICT, f)
        
    
def feed_keys_from_json(file: str = VALID_OPERATORS_FILE) -> dict:
    # First, open the JSON with all the operators
    with open(f"./{file}") as f:
        data = json.load(f)
    
    return data["VALID_FEED_KEYS"]


def check_for_completeness(
    export_path: str = CACHED_VIEWS_EXPORT_PATH, 
    all_feeds: list = [],
    analysis_date: str = analysis_date
) -> list:
    """
    Go through and check that the files are non-empty for all 4.
    
    Since stop_times is only downloaded if the first 3 are non-empty,
    just check for presence of stop_times file.
    
    Returns a potentially smaller list of operators that 
    we expect to run through rest of the HQTA workflow
    """
    FEEDS_WITH_FULL_INFO = [] 
    
    for feed_key in all_feeds:  
        stop_times = dd.read_parquet(
            f"{export_path}st_{feed_key}_{analysis_date}.parquet")

        if len(stop_times.index) > 0:  
            FEEDS_WITH_FULL_INFO.append(feed_key)
    
    return FEEDS_WITH_FULL_INFO

    
if __name__=="__main__":
    
    logger.add("./logs/operators_for_hqta.log", retention="6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
    # These are all the IDs that have some cached files in GCS
    operators_for_hqta = pd.read_parquet(
        f"{TEMP_GCS}operators_for_hqta.parquet")
    
    FEED_KEYS = operators_for_hqta.feed_keys.unique().tolist()
    
    time1 = dt.datetime.now()
    logger.info(f"get list of cached ITP IDs: {time1-start}")
    
    # Now check whether an operator has a complete set of files (len > 0)
    FEEDS_WITH_FULL_INFO = check_for_completeness(
        CACHED_VIEWS_EXPORT_PATH, FEED_KEYS, analysis_date)
    
    # Save that our dictionary to json, 
    # should be smaller than all operators with cached
    complete_operators = operators_for_hqta[
        opearators_for_hqta.feed_key.isin(FEEDS_WITH_FULL_INFO)]
    
    name_feed_key_dict_to_json(complete_operators, VALID_OPERATORS_FILE)
    
    time2 = dt.datetime.now()
    logger.info(f"check files for completeness, save as json: {time2-time1}")

    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

        
    
