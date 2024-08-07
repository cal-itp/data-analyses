"""
Aggregate to various grains.
"""
import dask.dataframe as dd
import datetime
import gcsfs
import pandas as pd
import uuid

from dask import delayed, compute
from pathlib import Path
from typing import Literal

import utils
from utils import RAW_GCS, PROCESSED_GCS
from crosswalks import station_id_cols

fs = gcsfs.GCSFileSystem()


def read_filepart_merge_crosswalk(
    filename: str,
    filepart: str, 
    **kwargs
) -> pd.DataFrame:
    """
    Import onen partition and merge in crosswalk
    """
    filepart_name = Path(filepart).name
    
    df = pd.read_parquet(
        f"{RAW_GCS}{filename}/{filepart_name}",
        **kwargs
    )
    
    crosswalk = pd.read_parquet(
        f"{PROCESSED_GCS}station_crosswalk.parquet"
    )
    
    df2 = pd.merge(
        df,
        crosswalk,
        on = station_id_cols,
        how = "inner",
    ).drop(columns = station_id_cols)
    
    return df2


def aggregate_metric(
    df: pd.DataFrame, 
    group_cols: list, 
    metric_name: Literal["flow", "truck_flow", "occ", "obs", "speed"] 
) -> pd.DataFrame:
    
    metric_cols = [c for c in df.columns if metric_name in c]
    
    if metric_name == "speed":
        metric_agg = "mean"
    else:
        metric_agg = "sum"
    
    if metric_name in ["occ", "speed", "obs_speed"]:
        metric_dtypes = {c: "Float64" for c in metric_cols}
    
    else:
        metric_dtypes = {c: "Int64" for c in metric_cols}
    
    df2 = (
        df
        .groupby(group_cols, 
                 group_keys=False)
        .agg(
            {**{c: metric_agg for c in metric_cols}}
        ).reset_index()
        .astype({
            **metric_dtypes,
            "year": "int16",
            "month": "int8",
            "weekday": "int8",
        })
    )
    
    if "hour" in df2.columns:
        df2 = df2.astype({"hour": "int8"})
    
    return df2


def metric_prep(metric: str) -> list:
    """
    Grab subset of columns related to a given metric.
    Merge in crosswalk.
    Do this across each partition, and return a list
    of delayed dfs that are have time columns, 
    ready to be aggregated.
    """
    filename = "hov_portion"
    list_of_files = fs.ls(f"{RAW_GCS}{filename}")
    
    all_columns = dd.read_parquet(
        f"{RAW_GCS}{filename}/",
        engine="pyarrow"
    ).columns.tolist()
    
    exclude_dict = {
        "flow": ["obs", "truck"],
        "occ": ["avg_occ"],
        "truck_flow": [],
        "obs_flow": [],
        "obs_speed": [],
        "speed": ["avg_speed", "obs"],
        "pts_obs": [],
    }
    
    metric_cols = [
        c for c in all_columns if f"_{metric}" in c and not
        any(word in c for word in exclude_dict[metric])
    ]

    import_dfs = [
        delayed(read_filepart_merge_crosswalk)(
            filename, 
            part_i, 
            columns = station_id_cols + ["time_id"] + metric_cols
        ) for part_i in list_of_files
    ]               

    time_dfs = [
        delayed(utils.parse_for_time_components)(i) for i in import_dfs
    ]
    
    time_dfs = [delayed(utils.add_peak_offpeak_column)(i, "hour") for i in time_dfs]
        
    return time_dfs


def compute_and_export(
    metric: str,
    metric_dfs: list,
    export_filename: str,
    **kwargs
):
    """
    Compute list of delayed aggregations,
    concatenate and export metric as parquet.
    """
    metric_dfs = [compute(i)[0] for i in metric_dfs]
    results = pd.concat(metric_dfs, axis=0, ignore_index=True)

    results.to_parquet(
        f"{PROCESSED_GCS}{export_filename}_{metric}.parquet",
        **kwargs
    )
    
    return
    
    
if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    metric_list = [
        "flow", "truck_flow", "obs_flow",
        "occ", 
        "speed", "obs_speed", # mean
        "pts_obs",
    ] 

    
    station_cols = ["station_uuid"]
    weekday_hour_cols = ["year", "month", "weekday", "hour"]
    weekday_peak_cols = ["year", "month", "weekday", "peak_offpeak"]

    for metric in metric_list:
        
        time0 = datetime.datetime.now()
        
        time_dfs = metric_prep(metric)
        
        hour_dfs = [
            delayed(aggregate_metric)(i, station_cols + weekday_hour_cols, metric)
            for i in time_dfs
        ]
        
        compute_and_export(
            metric, 
            hour_dfs, 
            "station_weekday_hour",
            partition_cols = ["weekday", "hour"]
        )
        
        time1 = datetime.datetime.now()
        print(f"{metric} hourly aggregation: {time1 - time0}")
    
        peak_dfs = [
            delayed(aggregate_metric)(i, station_cols + weekday_peak_cols, metric)
            for i in time_dfs
        ]
                
        compute_and_export(
            metric,
            peak_dfs,
            "station_weekday_peak",
            partition_cols = ["weekday", "peak_offpeak"]
        )

        time2 = datetime.datetime.now()
        print(f"{metric} peak/offpeak aggregation: {time2 - time1}")
        print(f"{metric} aggregation: {time2 - time0}")
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")