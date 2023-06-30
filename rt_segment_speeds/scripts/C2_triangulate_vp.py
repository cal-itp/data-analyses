"""
Starting from the vp that were spatially joined
to segments, pick a subset of these.
Triangulate these by picking 5 points to better 
calculate speeds for the entire trip.

If we pick only 2 points, for a looping route, origin/destination
are basically the same. If we pick 3 points, this is better 
for triangulating the distance traveled.
"""
import os
os.environ['USE_PYGEOS'] = '0'

import dask.dataframe as dd
import datetime
import numpy as np
import pandas as pd

from dask import delayed, compute
from typing import Literal

from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import (SEGMENT_GCS, #analysis_date,
                                              CONFIG_PATH
                                             )
from A2_valid_vehicle_positions import merge_usable_vp_with_sjoin_vpidx

analysis_date = "2023-05-17"

def trip_stat(
    ddf: dd.DataFrame, 
    group_cols: list, 
    stat: Literal["min", "max", "p25", "p50", "p75"]
) -> dd.DataFrame:
    """
    For a group, grab the min, max, and maybe some other
    quartiles.
    The shuffling done on all the vp is expensive, so
    we'll use vp_idx and roughly grab what's the 25th, 50th, 75th 
    percentile by taking the midpoints.
    
    If we're off by 1, it's not a big deal, since we're 
    sampling 5 vp per trip.
    """
    # observed = True means don't create rows when that 
    # group_cols combination is not present
    # we need this because gtfs_dataset_key is categorical dtype
    # group_keys = False so that group_cols is not used as index.
    grouped_df = ddf.groupby(group_cols, observed=True, group_keys=False)
    col = "vp_idx"
    
    def integrify(df: dd.DataFrame) -> dd.DataFrame:
        """
        Make sure vp_idx returns as an integer and get rid of any NaNs. 
        """
        df2 = df.dropna().astype("int64").reset_index()
        
        return df2
    
    if stat == "min":
        stat_df = grouped_df[col].min()
    
    elif stat == "max":
        stat_df = grouped_df[col].max()
    
    elif stat == "p50":
        stat_df = grouped_df[col].mean().round(0)
    
    elif stat == "p25":
        # medians, percentiles aren't included in dask aggregation, 
        # would have to do custom aggregation
        # and it's expensive because of the shuffling. 
        # do a rough version to approximate midpoint, 
        # since vp_idx should increase by 1
        stat_df = (
            (grouped_df[col].mean() - grouped_df[col].min()).divide(2).round(0) + 
            grouped_df[col].min()
        )
    
    elif stat == "p75":
        stat_df = (
            (grouped_df[col].max() - grouped_df[col].mean()).divide(2) + 
            grouped_df[col].mean()
        ).round(0)  
        
    return stat_df.pipe(integrify)
    

def triangulate_vp(
    ddf: dd.DataFrame, 
    group_cols: list = [
        "gtfs_dataset_key", "trip_id"]
) -> np.ndarray:
    """
    Grab a sample of vehicle positions for each trip to triangulate distance.
    These vp already sjoined onto the shape.
    Roughly pick vp at equally spaced intervals.
    
    Dask aggregation can't group and use lambda to create list of possible 
    vp_idx.
    Rather than caring about specifically which vp_idx, we just want
    roughly spaced apart ones that approximate 0, 25, 50, 75, 100 percentiles.
    """
    t0 = datetime.datetime.now()

    
    results = [delayed(trip_stat)(ddf, group_cols, s)
               for s in ["min", "p25", "p50", "p75", "max"]] 
    
    t1 = datetime.datetime.now()
    print(f"delayed stats: {t1 - t0}")
    
    results2 = [compute(i)[0].vp_idx.to_numpy() for i in results]
            
    t2 = datetime.datetime.now()
    print(f"compute, get np arrays: {t2 - t1}")
    
    # Here, row_stacking it results in 3 arrays, so flatten it to be 1d
    # or, equivalently, use np.concatenate
    stacked_results = np.concatenate(results2)
    
    return stacked_results


def subset_usable_vp(dict_inputs: dict) -> np.ndarray:
    """
    Subset all the usable vp and keep a sample of triangulated
    vp per trip.
    """
    SEGMENT_FILE = f'{dict_inputs["segments_file"]}_{analysis_date}'
    SJOIN_FILE = f'{dict_inputs["stage2"]}_{analysis_date}'
    USABLE_FILE = f'{dict_inputs["stage1"]}_{analysis_date}'
    GROUPING_COL = dict_inputs["grouping_col"]
    
    all_shapes = pd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}.parquet",
        columns = ["shape_array_key"]
    ).shape_array_key.unique().tolist()
    
    # Use this function to attach the crosswalk of sjoin results
    # back to usable_vp
    ddf = merge_usable_vp_with_sjoin_vpidx(
        all_shapes,
        USABLE_FILE,
        SJOIN_FILE,
        GROUPING_COL,
        columns = ["gtfs_dataset_key", "trip_id", "vp_idx"]
    )
    
    # Results are just vp_idx as np array
    results = triangulate_vp(
        ddf, 
        ["gtfs_dataset_key", "trip_id"]
    )
    
    return results


def merge_rt_scheduled_trips(
    rt_trips: dd.DataFrame,
    analysis_date: str,
    group_cols: list = ["trip_id"]) -> dd.DataFrame:
    """
    Merge RT trips (vehicle positions) to scheduled trips 
    to get the shape_array_key.
    Don't pull other scheduled trip columns now, wait until
    after aggregation is done.
    """
    keep_cols = [
        "feed_key",
        "shape_array_key", 
    ] + group_cols
        
    crosswalk = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, 
        keep_trip_cols = keep_cols, 
        get_pandas = True
    )
        
    df = dd.merge(
        rt_trips,
        crosswalk,
        on = ["gtfs_dataset_key"] + group_cols,
        how = "left",
    )
    
    return df


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    results = subset_usable_vp(STOP_SEG_DICT)
        
    time1 = datetime.datetime.now()
    print(f"compute results: {time1 - start}")
    
    # Use these vp_idx and filter the vp with all the columns
    vp_idx_list = results.tolist()
    
    USABLE_FILE = f'{STOP_SEG_DICT["stage1"]}_{analysis_date}'

    vp_results = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        USABLE_FILE,
        file_type = "df",
        partitioned = True,
        columns = ["gtfs_dataset_key", "_gtfs_dataset_name", "trip_id",
                   "location_timestamp_local",
                   "x", "y", "vp_idx"
                  ],
        filters = [[("vp_idx", "in", vp_idx_list)]]
    ).compute()
    
    vp_with_sched = (
        merge_rt_scheduled_trips(
            vp_results, 
            analysis_date, 
            group_cols = ["trip_id"]
        ).sort_values("vp_idx")
        .reset_index(drop=True)
    )
    
    vp_with_sched.to_parquet(
        f"{SEGMENT_GCS}trip_summary/vp_subset_{analysis_date}.parquet",
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
