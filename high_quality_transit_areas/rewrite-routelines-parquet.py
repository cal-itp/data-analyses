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

if __name__ == "__main__":
    ITP_IDS_TO_FIX = (tbl.gtfs_schedule.agency()
                      >> select(_.calitp_itp_id)
                      >> distinct()
                      >> collect()
                     ).calitp_itp_id.tolist()

    for itp_id in ITP_IDS_TO_FIX:
        open_geoparquet_rewrite(itp_id)
