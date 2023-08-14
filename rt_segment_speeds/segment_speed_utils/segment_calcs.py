import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import numpy as np
import pandas as pd

from typing import Union

from shared_utils import rt_utils

# https://stackoverflow.com/questions/58145700/using-groupby-to-store-value-counts-in-new-column-in-dask-dataframe
# https://github.com/dask/dask/pull/5327
def keep_min_max_timestamps_by_segment(
    vp_to_seg: dd.DataFrame, 
    segment_identifier_cols: list,
    timestamp_col: str = "location_timestamp_local"
) -> dd.DataFrame:
    """
    For each segment-trip combination, throw away excess points, just 
    keep the enter/exit points for the segment.
    """
    # a groupby including gtfs_dataset_key explodes the groupers, and 
    # a lot of NaNs result...why?
    # gtfs_dataset_key or name sometimes is category dtype...so we must use groupby(observed=True)
    # shape_array_key will uniquely identify feed_key + shape_id, so should be ok
    dtypes_map = vp_to_seg[segment_identifier_cols + [timestamp_col]].dtypes.to_dict()
    
    # https://stackoverflow.com/questions/52552066/dask-compute-gives-attributeerror-series-object-has-no-attribute-encode    
    grouped_df = vp_to_seg.groupby(segment_identifier_cols, 
                                   observed=True, group_keys=False)
    
    enter = (grouped_df
             [timestamp_col].min()
             .reset_index()
             # we lose the dtypes for int16 in dask, set it again
             .astype(dtypes_map)
            )

    exit = (grouped_df
            [timestamp_col].max()
            .reset_index()
            .astype(dtypes_map)
           )
    
    enter_exit = dd.multi.concat([enter, exit], axis=0)
    
    # Merge back in with original to only keep the min/max timestamps
    # dask can't sort by multiple columns to drop
    vp_full_info = dd.merge(
        vp_to_seg,
        enter_exit,
        on = segment_identifier_cols + [timestamp_col],
        how = "inner"
    ).reset_index(drop=True)
            
    return vp_full_info
    


def derive_speed(
    df: pd.DataFrame, 
    distance_cols: tuple = ("min_dist", "max_dist"), 
    time_cols: tuple = ("min_time", "max_time")
) -> pd.DataFrame:
    """
    Derive meters and sec elapsed to calculate speed_mph.
    """
    min_dist, max_dist = distance_cols[0], distance_cols[1]
    min_time, max_time = time_cols[0], time_cols[1]    
    
    df = df.assign(
        meters_elapsed = df[max_dist] - df[min_dist]
    )
    
    if df[min_time].dtype in ["float", "int"]:
        # If 2 time cols are already converted to seconds, just take difference
        df = df.assign(
            sec_elapsed = (df[max_time] - df[min_time])
        )
    else:
        # If 2 time cols are datetime, convert timedelta to seconds
        df = df.assign(
            sec_elapsed = (df[max_time] - df[min_time]).divide(
                           np.timedelta64(1, 's')),
        )
    
    df = df.assign(
        speed_mph = (df.meters_elapsed.divide(df.sec_elapsed) * 
                     rt_utils.MPH_PER_MPS)
    )
    
    return df


def calculate_speed_by_segment_trip(
    df: dd.DataFrame, 
    segment_identifier_cols: list,
    timestamp_col: str
) -> dd.DataFrame:
    """
    For each segment-trip pair, calculate find the min/max timestamp
    and min/max shape_meters. Use that to derive speed column.
    """ 
    segment_trip_cols = [
        "gtfs_dataset_key", "gtfs_dataset_name", 
        "trip_id", "trip_instance_key", "schedule_gtfs_dataset_key"
    ] + segment_identifier_cols
    
    grouped_df = df.groupby(segment_trip_cols, observed=True, group_keys=False)
    
    min_time_dist = (grouped_df
                     .agg({timestamp_col: "min", 
                           "shape_meters": "min"})
                     .reset_index()
                     .rename(columns = {
                         timestamp_col: "min_time",
                         "shape_meters": "min_dist"})
    )
    
    max_time_dist = (grouped_df
                     .agg({timestamp_col: "max", 
                           "shape_meters": "max"})
                     .reset_index()
                     .rename(columns = {
                         timestamp_col: "max_time",
                         "shape_meters": "max_dist"})
    )
    
    segment_trip_agg = pd.merge(
        min_time_dist,
        max_time_dist,
        on = segment_trip_cols, 
        how = "left"
    )
    
    #segment_trip_agg = dd.from_pandas(segment_trip_agg, npartitions=3)
    
    segment_speeds = derive_speed(
        segment_trip_agg,
        distance_cols = ("min_dist", "max_dist"),
        time_cols = ("min_time", "max_time")
    )
        
    return segment_speeds

                                   
def convert_timestamp_to_seconds(
    df: pd.DataFrame, 
    timestamp_cols: list,
) -> dd.DataFrame: 
    """
    Convert timestamp into seconds.
    """
    for c in timestamp_cols:
        df = df.assign(
            time_sec = ((df[c].dt.hour * 3_600) + 
                            (df[c].dt.minute * 60) + 
                            (df[c].dt.second)
                       ),
        ).rename(columns = {"time_sec": f"{c}_sec"})
    
    return df


def derive_stop_delay(
    df: dd.DataFrame, 
    rt_sched_time_cols: tuple = ("max_time_sec", "arrival_sec")
) -> dd.DataFrame:
    """
    Calculate the difference in actual arrival time 
    and scheduled arrival time.
    """
    actual, scheduled = rt_sched_time_cols[0], rt_sched_time_cols[1]
    
    df = df.assign(
        actual_minus_scheduled_sec = df[actual] - df[scheduled]
    )
    
    return df