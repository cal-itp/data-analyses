"""
Do a check of the operators to run and return a list.

Setting a list ahead of time vs running the tbl.gtfs_schedule.agency query
in the B1_bus_corridors script + adding more if/else/try/except statements.
Want to be explicit in what operators should be able to run, ones we
expect not to run, and ones we expect should run and are erroring.

Return a list of operators that have cached files, and 
thus should be able to have their HQTA corridors compiled.
"""
import json
import pandas as pd

from siuba import *

from calitp.tables import tbl
from shared_utils import rt_utils
from A1_rail_ferry_brt import analysis_date

date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)
HQTA_OPERATORS_FILE = "./hqta_operators.json"


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


def get_list_of_cached_itp_ids():
    ALL_ITP_IDS = (tbl.gtfs_schedule.agency()
               >> distinct(_.calitp_itp_id)
               >> filter(_.calitp_itp_id != 200, 
                         # Amtrak is always filtered out
                         _.calitp_itp_id != 13)
               >> collect()
    ).calitp_itp_id.tolist()
    
    
    ITP_IDS_WITH_CACHED_FILES = []

    for itp_id in ALL_ITP_IDS:
        cached_response = rt_utils.check_cached(
            f"routelines_{itp_id}_{date_str}.parquet", subfolder="cached_views/")

        if cached_response is not None:
            ITP_IDS_WITH_CACHED_FILES.append(itp_id)
    
    return sorted(ITP_IDS_WITH_CACHED_FILES)


def get_valid_itp_ids(file=HQTA_OPERATORS_FILE):
    with open(f"./{file}") as f:
        data = json.load(f)
        
    return data["VALID_ITP_IDS"]

    
if __name__=="__main__":
    ITP_IDS = get_list_of_cached_itp_ids()
    
    # Turn list into a dict, then save as json
    VALID_ITP_IDS_DICT = {}
    VALID_ITP_IDS_DICT["VALID_ITP_IDS"] = ITP_IDS
    
    # Write it out as json
    with open(f"{HQTA_OPERATORS_FILE}", "w") as f:
        json.dump(VALID_ITP_IDS_DICT, f)