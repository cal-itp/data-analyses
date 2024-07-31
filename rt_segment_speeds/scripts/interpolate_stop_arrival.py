"""
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from segment_speed_utils import (array_utils, helpers, 
                                 segment_calcs, wrangle_shapes)
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import PROJECT_CRS, SEGMENT_TYPES
from shared_utils import rt_dates

def get_vp_timestamps(
    input_file: str,
    analysis_date: str, 
    **kwargs
) -> pd.DataFrame:
        
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}{input_file}_{analysis_date}",
        columns = ["vp_idx", "location_timestamp_local", 
                   "moving_timestamp_local"],
        **kwargs
    )
        
    return vp 


def consolidate_surrounding_vp(
    df: pd.DataFrame, 
    group_cols: list,
) -> pd.DataFrame:
    """
    This reshapes the df to wide so that each stop position has
    a prior and subseq timestamp (now called vp_timestamp_local).
    """
    df = df.assign(
        obs = (df.sort_values(group_cols + ["vp_idx"])
               .groupby(group_cols, 
                        observed=True, group_keys=False, dropna=False)
               .cumcount()
            )
    )
    
    group_cols2 = group_cols + ["stop_meters"]
    prefix_cols = ["vp_idx", "shape_meters"]
    timestamp_cols = ["location_timestamp_local", "moving_timestamp_local"]
    # since shape_meters actually might be decreasing as time progresses,
    # (bus moving back towards origin of shape)
    # we don't actually know that the smaller shape_meters is the first timestamp
    # nor the larger shape_meters is the second timestamp.
    # all we know is that stop_meters (stop) falls between these 2 shape_meters.
    # sort by timestamp, and set the order to be 0, 1    
    vp_before_stop = df.loc[df.obs==0][group_cols2 + prefix_cols + timestamp_cols]
    vp_after_stop = df.loc[df.obs==1][group_cols2 + prefix_cols + timestamp_cols]
    
    # For the vp before the stop occurs, we want the maximum timestamp
    # of the last position
    # We want to keep the moving_timestamp (which is after it's dwelled)
    vp_before_stop = vp_before_stop.assign(
        prior_vp_timestamp_local = vp_before_stop.moving_timestamp_local,
    ).rename(
        columns = {**{i: f"prior_{i}" for i in prefix_cols}}
    ).drop(columns = timestamp_cols)
    
    # For the vp after the stop occurs, we want the minimum timestamp
    # of that next position
    # Keep location_timetamp (before it dwells)
    vp_after_stop = vp_after_stop.assign(
        subseq_vp_timestamp_local = vp_after_stop.location_timestamp_local,
    ).rename(
        columns = {**{i: f"subseq_{i}" for i in prefix_cols}}
    ).drop(columns = timestamp_cols)
    
    df_wide = pd.merge(
        vp_before_stop,
        vp_after_stop,
        on = group_cols2,
        how = "inner"
    )

    return df_wide


def add_arrival_time(
    nearest_vp_input_file: str,
    vp_timestamp_file: str,
    analysis_date: str,
    group_cols: list
) -> pd.DataFrame:    
    """
    Take the 2 nearest vp and transfrom df so that every stop
    position has a prior and subseq vp_idx and timestamps.
    This makes it easy to set up our interpolation of arrival time.
    Arrival time should be between moving_timestamp of prior
    and location_timestamp of subseq.
    """
    vp_filtered = pd.read_parquet(
        f"{SEGMENT_GCS}{nearest_vp_input_file}_{analysis_date}.parquet"
    )
    
    subset_vp = vp_filtered.vp_idx.unique()
    
    vp_timestamps = get_vp_timestamps(
        vp_timestamp_file, 
        analysis_date, 
        filters = [[("vp_idx", "in", subset_vp)]]
    )
    
    df = pd.merge(
        vp_filtered,
        vp_timestamps,
        on = "vp_idx",
        how = "inner"
    ).pipe(consolidate_surrounding_vp, group_cols)
        
    arrival_time_series = []
    
    for row in df.itertuples():
        
        stop_position = getattr(row, "stop_meters")
        
        projected_points = np.asarray([
            getattr(row, "prior_shape_meters"), 
            getattr(row, "subseq_shape_meters")
        ])
        
        timestamp_arr = np.asarray([
            getattr(row, "prior_vp_timestamp_local"),
            getattr(row, "subseq_vp_timestamp_local"),
        ])
        
        
        interpolated_arrival = wrangle_shapes.interpolate_stop_arrival_time(
            stop_position, projected_points, timestamp_arr)
        
        arrival_time_series.append(interpolated_arrival)
        
    df["arrival_time"] = arrival_time_series
    
    drop_cols = [i for i in df.columns if 
                 ("prior_" in i) or ("subseq_" in i)]
    
    df2 = df.drop(columns = drop_cols)
    
    del df, arrival_time_series
    
    return df2


def stop_and_arrival_time_arrays_by_trip(
    df: pd.DataFrame, 
    trip_stop_cols: list
) -> pd.DataFrame:
    """
    For stops that violated the monotonically increasing condition,
    set those arrival_times to NaT again.
    Now, look across stops and interpolate again, using stop_meters.
    """
    # Add columns with the trip's stop_meters and arrival_times
    # for only correctly interpolated values
    df_arrays = (df[df.arrival_time.notna()].sort_values(trip_stop_cols)
           .groupby("trip_instance_key", group_keys=False)
           .agg({
               "stop_meters": lambda x: list(x),
               "arrival_time": lambda x: list(x)
           }).rename(columns = {
               "stop_meters": "stop_meters_arr", 
               "arrival_time": "arrival_time_arr"
           }).reset_index()
    )
    
    df2 = pd.merge(
        df,
        df_arrays,
        on = "trip_instance_key",
        how = "inner"
    )

    # Use correct values to fill in the missing arrival times
    df2 = df2.assign(
        arrival_time = df2.apply(
            lambda x: wrangle_shapes.interpolate_stop_arrival_time(
                x.stop_meters, x.stop_meters_arr, x.arrival_time_arr
            ), axis=1
        )
    ).drop(columns = ["stop_meters_arr", "arrival_time_arr"])

    return df2


def enforce_monotonicity_and_interpolate_across_stops(
    df: pd.DataFrame,
    trip_stop_cols: list
) -> pd.DataFrame:
    """
    Do a check to make sure stop arrivals are all monotonically increasing.
    If it fails the check in a window of 3, and the center
    position is not increasing, we will interpolate again using 
    surrounding observations.
    """
    df = segment_calcs.convert_timestamp_to_seconds(
        df, ["arrival_time"])

    df = array_utils.rolling_window_make_array(
        df, 
        window = 3, rolling_col = "arrival_time_sec"
    )
    
    # Subset to trips that have at least 1 obs that violates monotonicity
    trips_with_one_false = (df.groupby("trip_instance_key")
                        .agg({"arrival_time_sec_monotonic": "min"})
                        .reset_index()
                        .query('arrival_time_sec_monotonic==0')
                        .trip_instance_key
                        )
    
    # Set arrival times to NaT if it's not monotonically increasing
    mask = df.arrival_time_sec_monotonic == False 
    df.loc[mask, 'arrival_time'] = np.nan
    
    no_fix = df[~df.trip_instance_key.isin(trips_with_one_false)]
    fix1 = df[df.trip_instance_key.isin(trips_with_one_false)]
    fix1 = stop_and_arrival_time_arrays_by_trip(fix1, trip_stop_cols)
    
    drop_me = [
        "arrival_time_sec",
        "rolling_arrival_time_sec", "arrival_time_sec_monotonic"
    ]
    
    fixed_df = pd.concat(
        [no_fix, fix1], axis=0
    ).drop(
        columns = drop_me
    ).sort_values(
        trip_stop_cols
    ).reset_index(drop=True)
        
    return fixed_df


def interpolate_stop_arrivals(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional = GTFS_DATA_DICT
):
    dict_inputs = config_path[segment_type]
    trip_stop_cols = [*dict_inputs["trip_stop_cols"]]
    USABLE_VP_FILE = dict_inputs["stage1"]
    INPUT_FILE = dict_inputs["stage2b"]
    STOP_ARRIVALS_FILE = dict_inputs["stage3"]

    start = datetime.datetime.now()
    
    df = delayed(add_arrival_time)(
        INPUT_FILE, 
        USABLE_VP_FILE,
        analysis_date,
        trip_stop_cols + ["shape_array_key"]   
    )
    
    results = delayed(enforce_monotonicity_and_interpolate_across_stops)(
        df, trip_stop_cols)
    
    results = compute(results)[0]
        
    results.to_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}_{analysis_date}.parquet"
    )
        
    end = datetime.datetime.now()
    logger.info(f"interpolate arrivals for {segment_type} "
                f"{analysis_date}:  {analysis_date}: {end - start}") 
        
    del results, df
    
    return

'''
if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
        
    delayed_dfs = [
        delayed(interpolate_stop_arrivals)(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        ) for analysis_date in analysis_date_list
    ]
    
    [compute(i)[0] for i in delayed_dfs]
'''