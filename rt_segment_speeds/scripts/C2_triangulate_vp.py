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

from typing import Literal

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date,
                                              CONFIG_PATH)

from A3_valid_vehicle_positions import merge_usable_vp_with_sjoin_vpidx


def triangulate_vp(
    ddf: dd.DataFrame, 
    group_cols: list = ["trip_instance_key"]
) -> np.ndarray:
    """
    Grab a sample of vehicle positions for each trip to triangulate distance.
    These vp already sjoined onto the shape.
    Roughly pick vp at equally spaced intervals.
    
    Dask aggregation can't group and use lambda to create list of possible 
    vp_idx.
    """        
    grouped_ddf = ddf.groupby(group_cols, observed=True, group_keys=False)

    min_df = (grouped_ddf
              .agg({"vp_idx": "min"})
              .rename(columns = {"vp_idx": "min_vp_idx"})
             )

    max_df = (grouped_ddf
              .agg({"vp_idx": "max"})
              .rename(columns = {"vp_idx": "max_vp_idx"})
             )
    
    vp_range = dd.merge(
        min_df,
        max_df,
        left_index = True,
        right_index = True,
        how = "inner"
    )

    vp_range = vp_range.persist()
    
    vp_range["range_diff"] = vp_range.max_vp_idx - vp_range.min_vp_idx
    
    vp_range = vp_range.assign(
        p25_vp_idx = (vp_range.range_diff * 0.25 + vp_range.min_vp_idx
                     ).round(0).astype("int64"),
        p50_vp_idx = (vp_range.range_diff * 0.5 + vp_range.min_vp_idx
                     ).round(0).astype("int64"),
        p75_vp_idx = (vp_range.range_diff * 0.75 + vp_range.min_vp_idx
                     ).round(0).astype("int64"),
    )
    
    vp_idx_cols = [
        "min_vp_idx", 
        "p25_vp_idx",
        "p50_vp_idx", 
        "p75_vp_idx",
        "max_vp_idx"
    ]

    results = vp_range[vp_idx_cols].compute().to_numpy().flatten()    
    
    return results


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
        USABLE_FILE,
        SJOIN_FILE,
        sjoin_filtering = [(GROUPING_COL, "in", all_shapes)],
        columns = ["trip_instance_key", "vp_idx"]
    )
    
    # Results are just vp_idx as np array
    results = triangulate_vp(
        ddf, 
        ["trip_instance_key"]
    )
    
    return results


def merge_rt_scheduled_trips(
    rt_trips: dd.DataFrame,
    analysis_date: str,
    group_cols: list = ["trip_instance_key"]) -> dd.DataFrame:
    """
    Merge RT trips (vehicle positions) to scheduled trips 
    to get the shape_array_key.
    Don't pull other scheduled trip columns now, wait until
    after aggregation is done.
    """
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = group_cols + ["shape_array_key"],
        get_pandas = True
    )
        
    df = dd.merge(
        rt_trips,
        trips,
        on = group_cols,
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

    vp_results = dd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_FILE}/", 
        columns = [
            "gtfs_dataset_key", "trip_instance_key",
            "location_timestamp_local",
            "x", "y", "vp_idx"],
        filters = [[("vp_idx", "in", vp_idx_list)]]
    ).compute()

    vp_with_sched = (
        merge_rt_scheduled_trips(
            vp_results, 
            analysis_date, 
            group_cols = ["trip_instance_key"]
        ).sort_values("vp_idx")
        .reset_index(drop=True)
    )
    
    vp_with_sched.to_parquet(
        f"{SEGMENT_GCS}trip_summary/vp_subset_{analysis_date}.parquet",
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
