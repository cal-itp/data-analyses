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
from shared_utils import publish_utils

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
    """
    Aggregate metric (mean preferred for now) 
    against a list of grouping columns.
    """
    metric_cols = [c for c in df.columns if metric_name in c]
        
    df2 = (
        df
        .groupby(group_cols, 
                 group_keys=False)
        .agg(
            {**{c: "mean" for c in metric_cols}}
        ).reset_index()
        .astype({
            # since everything is mean, use floats, but allow NaNs
            {c: "Float64" for c in metric_cols}
        })
    )
    
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
    
    # When we are interested in 1 particular metric, 
    # we should look for columns that contain a keyword
    # but remove confounding ones (metric = flow; remove obs_flow, truck_flow)
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

    # Create list of delayed dfs where we read in 1 partition 
    # and subset the columns
    # Note: dd.read_parquet() has dtype errors 
    import_dfs = [
        delayed(read_filepart_merge_crosswalk)(
            filename, 
            part_i, 
            columns = station_id_cols + ["time_id"] + metric_cols
        ) for part_i in list_of_files
    ]               

    # Add additional time columns we want 
    time_dfs = [
        delayed(utils.parse_for_time_components)(i) for i in import_dfs
    ]
    
    time_dfs = [delayed(utils.add_peak_offpeak_column)(i, "hour") 
                for i in time_dfs]
        
    time_dfs = [
        delayed(utils.add_weekday_weekend_column)(i, "weekday") 
        for i in time_dfs
    ] 
        
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
    
    for c in ["hour", "month", "weekday"]:
        if c in results.columns:
            results = results.astype({c: "int8"})
    if "year" in results.columns:
        results = results.astype({"year": "int16"})

    results.to_parquet(
        f"{PROCESSED_GCS}{export_filename}_{metric}.parquet",
        **kwargs
    )
    
    return

def process_one_metric(
    metric_name: Literal["flow", "truck_flow", "occ", "obs", "speed"],
    group_cols: list,
    export_filename: str
):
    """
    Prep and aggregate one metric and save out at particular grain.
    """
    time0 = datetime.datetime.now()
    
    time_dfs = metric_prep(metric_name)
        
    aggregated_dfs = [
        delayed(aggregate_metric)(i, group_cols, metric_name)
        for i in time_dfs
    ] 
        
    publish_utils.if_exists_then_delete(
        f"{PROCESSED_GCS}{export_filename}_{metric_name}.parquet"
    )       
    
    compute_and_export(
        metric_name, 
        aggregated_dfs, 
        export_filename,
    )
    
    time1 = datetime.datetime.now()
    print(f"{metric_name} exported {export_filename}: {time1 - time0}")
   
    return


def import_detector_status(
    filename: str = "hov_portion_detector_status_time_window",
) -> pd.DataFrame:
    """
    Import detector partitioned df,
    parse time_id, and merge in crosswalk to get station_uuid.
    """
    df = pd.read_parquet(
        f"{RAW_GCS}{filename}/",
        columns = ["time_id", "station_id", "all_samples"],
    ).pipe(
        utils.parse_for_time_components
    ).pipe(
        utils.add_peak_offpeak_column, "hour"
    ).pipe(
        utils.add_weekday_weekend_column, "weekday"
    )
    
    # Merge in station_uuid
    crosswalk = pd.read_parquet(
        f"{PROCESSED_GCS}station_crosswalk.parquet",
        columns = ["station_id", "station_uuid"]
    )
    
    df2 = pd.merge(
        df,
        crosswalk,
        on = "station_id",
        how = "inner"
    ).drop(columns = "station_id")
    
    return df2


def aggregate_detector_samples(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Right now, only all_samples is a column that's understood
    all_samples = diag_samples, so we'll keep just one column.
    
    We want to know other metrics like:
    status (0/1) - which one is good and which one is bad?
    suspected_err (integers, 0-9 inclusive)
    certain errors like high_flow, zero_occ, etc, but not sure how to interpret
    """
    df2 = (df.groupby(group_cols, group_keys=False)
           .agg({
               "all_samples": "sum",
           }).reset_index()
    )
    
    return df2
    
    
if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    metric_list = [
        "flow", "truck_flow", "obs_flow",
        "occ", "speed", "obs_speed", "pts_obs",
    ] 

    
    station_cols = ["station_uuid"]
    
    GRAINS = {
        "station_weekday_hour": station_cols + ["year", "month", "weekday", "hour"],
        "station_weekday_peak": station_cols + ["year", "month", "weekday", "peak_offpeak"],
        "station_daytype_hour": station_cols + ["hour", "daytype"]
    }
        
    for metric in metric_list:
                
        for export_filename, grain_cols in GRAINS.items():
            
            process_one_metric(metric, grain_cols, export_filename)

    
    detector_df = import_detector_status()
    
    for export_filename, grain_cols in GRAINS.items():

        agg_df = aggregate_detector_samples(
            detector_df, 
            grain_cols
        )

        agg_df.to_parquet(
            f"{PROCESSED_GCS}{export_filename}_detectors.parquet"
        )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")