"""
Concatenate CSVs in Traffic Ops data dump 
to be parquets.
"""
import dask.dataframe as dd
import datetime
import gcsfs
import pandas as pd

from dask import delayed, compute
from pathlib import Path
from typing import Union

from calitp_data_analysis.sql import to_snakecase
from utils import RAW_GCS

fs = gcsfs.GCSFileSystem()

def import_csv_export_parquet(file_name: Union[str, Path]):
    """
    Import csv and write out as parquet in staging folder.
    """
    lane_cols = (
        [f"LANE_{i}_FLOW" for i in range(1, 9)] + 
        [f"LANE_{i}_TRUCK_FLOW" for i in range(1, 9)]  
    )
    
    # Certain columns seemed to want float because there were NaNs
    # while using Dask. Let's just set it to Int64
    df = pd.read_csv(
        file_name, index_col=0,
        dtype={
            "CITY_ID": "float64",
            **{c: "Int64" for c in lane_cols}
        }
    )
    
    # This seems to be the only datetime column
    df = df.assign(
        TIME_ID = pd.to_datetime(df.TIME_ID)
    ).pipe(to_snakecase)
        
    df.to_parquet(
        f"{RAW_GCS}staging/{Path(file_name).stem}.parquet"
    )
    
    return


def concatenate_files_in_folder(file_name_pattern: str):
    """
    Find all the files in folder and write out staging 
    parquets, then concatenate together into a
    partitioned parquet.
    """
    files = fs.glob(f"{RAW_GCS}{file_name_pattern}.csv")
    files = [f"gs://{f}" for f in files]
    
    file_name = file_name_pattern.replace("_*_", "_")
    
    # Loop through files to import/export
    dfs = [
        delayed(import_csv_export_parquet)(f) 
        for f in files
    ]
    
    # This saves it out the files
    [compute(i)[0] for i in dfs]
    
    # Read in all the parquets in staging/ and 
    # repartition to go from 100+ -> fewer partitions
    df = dd.read_parquet(
        f"{RAW_GCS}staging/{Path(file_name_pattern).stem}.parquet",
        dtype = {"time_id": "datetime64"}
    )
    
    if "detector" in file_name:
        df = df.repartition(npartitions=5)
    else:
        df = df.repartition(npartitions=25)
    
    df.to_parquet(
        f"{RAW_GCS}{Path(file_name).stem}", 
        engine="pyarrow"
    )
    
    print(f"exported {Path(file_name).stem}")
    

if __name__ == "__main__":
    
    file_names = [
        "hov_*_portion", 
        "hov_*_portion_detector_status_time_window"
    ]
    
    for f in file_names:
        
        start = datetime.datetime.now()
        
        concatenate_files_in_folder(f)
        
        end = datetime.datetime.now()
        print(f"save {f} as parquet: {end - start}")
    
    # Delete staging folder
    fs.rm(f"{RAW_GCS}staging/", recursive=True)
    
    
