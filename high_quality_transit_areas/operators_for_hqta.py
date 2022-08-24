"""
Do a check of the operators to run and return a list.

Setting a list ahead of time vs running the tbl.gtfs_schedule.agency query
in the B1_bus_corridors script + adding more if/else/try/except statements.
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

from siuba import *

from calitp.tables import tbl
from shared_utils import rt_utils
from update_vars import (analysis_date, date_str, CACHED_VIEWS_EXPORT_PATH, 
                         ALL_OPERATORS_FILE, VALID_OPERATORS_FILE)

'''
ITP_IDS_IN_GCS = [
    101, 102, 103, 105, 106, 108, 10, 110,
    112, 116, 117, 118, 11, 120, 
    121, 122, 123, 126, 127, 129,
    135, 137, 13, 142, 146, 148, 14,
    152, 154, 159, 15, 
    162, 165, 167, 168, 169, 16, 
    170, 171, 172, 173, 174, 176, 177, 178, 179, 17,
    181, 182, 183, 186, 187, 188, 18, 190, 
    192, 194, 198, 199, 
    201, 203, 204, 206, 207, 208, 
    210, 212, 213, 214, 217, 218, 21, 
    220, 221, 226, 228, 231, 232, 235,
    238, 239, 23, 243, 246, 247, 24, 
    251, 254, 257, 259, 260, 
    261, 263, 264, 265, 269, 
    270, 271, 273, 274, 278, 279,
    280, 281, 282, 284, 287, 289, 
    290, 293, 294, 295, 296, 298, 29, 
    300, 301, 305, 308, 30, 
    310, 312, 314, 315, 320, 323, 324, 327, 329, 
    331, 334, 336, 337, 338, 339, 33, 
    341, 343, 344, 346, 349, 34, 
    350, 351, 356, 35, 360, 361, 365,
    366, 367, 368, 36, 372, 374, 376, 37,
    380, 381, 386, 389, 394, 
    41, 42, 45, 473, 474,
    482, 483, 484, 48, 49, 4, 
    50, 54, 56, 61, 6, 
    70, 71, 75, 76, 77, 79, 
    81, 82, 83, 86, 87, 
    91, 95, 98, 99
]
'''

def get_list_of_cached_itp_ids(date_str: str, ALL_ITP_IDS: list = None) -> list:
    """
    ALL_ITP_IDS: list. 
                Allow passing an alternate list of IDs in. 
                If list is not specified, then run a fresh query.    
    """
    if ALL_ITP_IDS is None:
        ALL_ITP_IDS = (tbl.gtfs_schedule.agency()
                   >> distinct(_.calitp_itp_id)
                   >> filter(_.calitp_itp_id != 200, 
                             # Amtrak is always filtered out
                             _.calitp_itp_id != 13)
                   >> collect()
        ).calitp_itp_id.tolist()
    
    ITP_IDS_WITH_CACHED_FILES = []

    for itp_id in ALL_ITP_IDS:
        response1 = rt_utils.check_cached(
            f"routelines_{itp_id}_{date_str}.parquet", subfolder="cached_views/")
        response2 = rt_utils.check_cached(
            f"trips_{itp_id}_{date_str}.parquet", subfolder="cached_views/")
        response3 = rt_utils.check_cached(
            f"st_{itp_id}_{date_str}.parquet", subfolder="cached_views/")
        response4 = rt_utils.check_cached(
            f"stops_{itp_id}_{date_str}.parquet", subfolder="cached_views/")
        
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


def check_for_completeness(export_path: str = CACHED_VIEWS_EXPORT_PATH, 
                           all_operators_file: str = ALL_OPERATORS_FILE, 
                           valid_operators_file: str = VALID_OPERATORS_FILE):
    
    # First, open the JSON with all the operators
    ALL_ITP_IDS = itp_ids_from_json(all_operators_file)
    
    # Go through and check that the files are non-empty for all 4
    # If it is, then add it to our smaller list of operators that we expect to run through
    # rest of the HQTA workflow
    IDS_WITH_FULL_INFO = [] 
    
    for itp_id in ALL_ITP_IDS:  
        routelines = dask_geopandas.read_parquet(
                f"{export_path}routelines_{itp_id}_{date_str}.parquet")
        trips = dd.read_parquet(f"{export_path}trips_{itp_id}_{date_str}.parquet")
        stop_times = dd.read_parquet(f"{export_path}st_{itp_id}_{date_str}.parquet")
        stops = dask_geopandas.read_parquet(f"{export_path}stops_{itp_id}_{date_str}.parquet")
       
        if ( (len(routelines.index) > 0) and (len(trips.index) > 0) and 
            (len(stop_times.index) > 0) and (len(stops.index) > 0) ):    
            IDS_WITH_FULL_INFO.append(itp_id)
    
    return IDS_WITH_FULL_INFO

    
if __name__=="__main__":
    start = dt.datetime.now()
    
    # These are all the IDs that have some cached files in GCS
    ITP_IDS = get_list_of_cached_itp_ids(date_str)
    
    # Turn list into a dict, then save as json
    list_to_json(ITP_IDS, ALL_OPERATORS_FILE)
    
    time1 = dt.datetime.now()
    print(f"cached ids, save as json: {time1-start}")
    
    # Now check whether an operator has a complete set of files (len > 0)
    IDS_WITH_FULL_INFO = check_for_completeness(
        CACHED_VIEWS_EXPORT_PATH, ALL_OPERATORS_FILE, VALID_OPERATORS_FILE)
    
    # Save that list to json, should be smaller than all operators with cached
    list_to_json(IDS_WITH_FULL_INFO, VALID_OPERATORS_FILE)
    
    time2 = dt.datetime.now()
    print(f"check files for completeness, save as json: {time2-time1}")
    
    end = dt.datetime.now()
    print(f"execution time: {end-start}")

        
    
