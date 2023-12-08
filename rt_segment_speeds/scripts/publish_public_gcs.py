"""
Grab all files in the rollup
"""
import datetime
import gcsfs
import pandas as pd

from dask import delayed, compute
from segment_speed_utils.project_vars import SEGMENT_GCS, PUBLIC_GCS

fs = gcsfs.GCSFileSystem()

def concatenate_datasets_across_months(dataset_name: str) -> pd.DataFrame:
    list_of_files = fs.glob(f"{SEGMENT_GCS}rollup/{dataset_name}_*")
    print(list_of_files)
    
    dfs = [
        pd.read_parquet(f"gs://{d}").drop(
            columns = "schedule_gtfs_dataset_key"
        ) for d in list_of_files
    ]
    
    df = (pd.concat(dfs, 
                    axis=0, ignore_index=True)
          .sort_values(["organization_name",
                        "year", "month", "peak_offpeak", "weekday_weekend",
                        "shape_id", "stop_sequence"])
          .reset_index(drop=True)
         )
    
    return df

if __name__ == "__main__":

    DATASETS = [
        "speeds_by_peak_daytype"
    ]
    
    for d in DATASETS:
        start = datetime.datetime.now()

        df = delayed(concatenate_datasets_across_months)(d)
        df = compute(df)[0]
        df.to_parquet(f"{PUBLIC_GCS}speeds/{d}.parquet")
                
        end = datetime.datetime.now()
        print(f"save {d} to public GCS: {end - start}")