"""
Move the cached parquets (compiled across operators) into the same
folder: `rt_delay/compiled_cached_views/`

These are used in `traffic_ops` and `bus_service_increase` scripts for PMAC,
but should reside closer to where the individual operator parquets are
to minimize confusion.
"""
import geopandas as gpd
import pandas as pd
from typing import Literal

from shared_utils import utils, rt_dates

dates_in_gcs = [
    rt_dates.DATES["feb2022"], 
    rt_dates.DATES["may2022"], 
    rt_dates.DATES["jul2022"]
]

def move_parquets(dataset: Literal["trips", "st", "stops", "routelines"] = "trips", 
                  date: str = "2022-02-08", 
                  filename: str = "",
                  old_folder: str = "", 
                  new_folder: str = ""):
    
    if dataset in ["trips", "st"]:
        df = pd.read_parquet(f"{old_folder}{filename}.parquet")
        df.to_parquet(f"{new_folder}{filename}.parquet")
        print(f"Moved to: {new_folder}{filename}.parquet")
    
    elif dataset in ["stops", "routelines"]:
        df = gpd.read_parquet(f"{old_folder}{filename}.parquet")
        utils.geoparquet_gcs_export(df, 
                                    new_folder,
                                    filename
        )
        print(f"Moved to: {new_folder}{filename}.parqruet")

        
if __name__ == "__main__":   
    BUCKET = "gs://calitp-analytics-data/data-analyses/"
    
    for date in dates_in_gcs:
        for d in ["stops", "routelines", "trips", "st"]:
            for f in [f"{d}_{date}" , f"{d}_{date}_all"]:
                try:
                    move_parquets(
                        dataset = d, 
                        date = date, 
                        filename = f,
                        old_folder = f"{BUCKET}traffic_ops/",
                        #old_folder = f"{BUCKET}bus_service_increase/",
                        new_folder = f"{BUCKET}rt_delay/compiled_cached_views/",
                    )
                except:
                    pass
