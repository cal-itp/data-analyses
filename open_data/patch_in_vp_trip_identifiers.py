import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(600_000_000_000)

import geopandas as gpd
import pandas as pd

from calitp_data_analysis.tables import tbls
from siuba import *

from shared_utils import rt_dates, utils
from segment_speed_utils.project_vars import SEGMENT_GCS

def vp_trip_ids(analysis_date: str) -> pd.DataFrame: 
    vp = (tbls.mart_gtfs.fct_vehicle_locations()
          >> filter(_.service_date == analysis_date)
          >> select(_.gtfs_dataset_key, _.trip_id, 
                    _.trip_instance_key, 
                    _.schedule_gtfs_dataset_key
                   )
          >> distinct()
          >> collect()
         )
    print(vp.dtypes)
    
    vp.to_parquet(f"{SEGMENT_GCS}vp_patch_{analysis_date}.parquet")
    
    
def merge_vp_with_patch(date: str):
    # For earlier months, we would have treated a trip by trip_id only
    # not including trip_start_time
    # there are only about 10 instances where there are duplicates
    patch_df = pd.read_parquet(
        f"{SEGMENT_GCS}vp_patch_{date}.parquet"
    ).sort_values(
        ["gtfs_dataset_key", "trip_id", "trip_instance_key"]
    ).drop_duplicates(subset=["gtfs_dataset_key", "trip_id"])
    
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}vp_{date}.parquet",
    )
    
    df = pd.merge(
        vp,
        patch_df,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "left",
    )
    
    print(df.dtypes)
    utils.geoparquet_gcs_export(
        df,
        SEGMENT_GCS,
        f"vp_{date}"
    )
    
if __name__ == "__main__":
    dates_list = [
        "mar2023", "apr2023", 
        "may_2023", "jun2023"
    ]
    
    dates = [rt_dates.DATES[d] for d in dates_list]
    
    for d in dates:
        vp_trip_ids(d)
        merge_vp_with_patch(d)