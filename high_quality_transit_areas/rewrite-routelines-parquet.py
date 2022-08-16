"""
Routelines in GCS are named .parquet.parquet

Just change most recent analysis date and re-upload.

cached_views/routelines_{itp_id}_{date_str}.parquet.parquet
"""
import datetime as dt
import geopandas as gpd
import os

from shared_utils import rt_utils

from siuba import *
from calitp.tables import tbl
from calitp.storage import get_fs

fs = get_fs()

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/rt_delay"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

analysis_date = dt.date(2022, 5, 4) 

date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

def open_geoparquet_rewrite(itp_id):
    FILE_NAME = f"routelines_{itp_id}_{date_str}"

    WRONG_PATH = f"{GCS_FILE_PATH}cached_views/{FILE_NAME}.parquet.parquet"
    
    try:
        object_path = fs.open(WRONG_PATH)

        gdf = gpd.read_parquet(object_path)

        gdf.to_parquet(f"{FILE_NAME}.parquet")

        fs.put(f"./{FILE_NAME}.parquet", f"{GCS_FILE_PATH}cached_views/{FILE_NAME}.parquet")
        os.remove(f"./{FILE_NAME}.parquet")
        fs.delete(WRONG_PATH)
        
        print(f"Fixed {itp_id}")
        
    except:
        pass
    
'''
ITP_IDS_TO_FIX = [
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

if __name__ == "__main__":
    ITP_IDS_TO_FIX = (tbl.gtfs_schedule.agency()
                      >> select(_.calitp_itp_id)
                      >> distinct()
                      >> collect()
                     ).calitp_itp_id.tolist()

    for itp_id in ITP_IDS_TO_FIX:
        open_geoparquet_rewrite(itp_id)
