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
    # shape_array_key will uniquely identify feed_key + shape_id, so should be ok
    segment_trip_cols = ["trip_id"] + segment_identifier_cols
    dtypes_map = vp_to_seg[segment_trip_cols + [timestamp_col]].dtypes.to_dict()
    
    # https://stackoverflow.com/questions/52552066/dask-compute-gives-attributeerror-series-object-has-no-attribute-encode    
    enter = (vp_to_seg.groupby(segment_trip_cols)
             [timestamp_col].min()
             .reset_index()
             # we lose the dtypes for int16 in dask, set it again
             .astype(dtypes_map)
            )

    exit = (vp_to_seg.groupby(segment_trip_cols)
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
        on = segment_trip_cols + [timestamp_col],
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
        meters_elapsed = df[max_dist] - df[min_dist],
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
    segment_trip_cols = segment_identifier_cols + ["trip_id"]
    
    min_time_dist = (df.groupby(segment_trip_cols)
                     .agg({timestamp_col: "min", 
                           "shape_meters": "min"})
                     .reset_index()
                     .compute()
                     .rename(columns = {
                         timestamp_col: "min_time",
                         "shape_meters": "min_dist"})
    )
    
    max_time_dist = (df.groupby(segment_trip_cols)
                     .agg({timestamp_col: "max", 
                           "shape_meters": "max"})
                     .reset_index()
                     .compute()
                     .rename(columns = {
                         timestamp_col: "max_time",
                         "shape_meters": "max_dist"})
    )

    # groupby with gtfs_dataset_key/name explode the number of rows, 
    # so we won't use it in getting min/max time and distance
    # but we will merge it in with base_agg
    base_agg = (df[["gtfs_dataset_key", "_gtfs_dataset_name"] + 
                    segment_trip_cols]
                .drop_duplicates()
                .reset_index(drop=True)
               )
    
    segment_trip_agg = (dd.merge(
        base_agg,
        min_time_dist,
        on = segment_trip_cols, 
        how = "left"
    ).merge(max_time_dist, on = segment_trip_cols, how = "left")
    )
    
    segment_speeds = derive_speed(
        segment_trip_agg,
        distance_cols = ("min_dist", "max_dist"),
        time_cols = ("min_time", "max_time")
    )
        
    return segment_speeds

                                   
def convert_timestamp_to_seconds(
    df: pd.DataFrame, 
    timestamp_col: str,
) -> dd.DataFrame: 
    """
    Convert timestamp into seconds.
    """
    df = df.assign(
        time_sec = ((df[timestamp_col].dt.hour * 3_600) + 
                        (df[timestamp_col].dt.minute * 60) + 
                        (df[timestamp_col].dt.second)
                   ),
    ).rename(columns = {"time_sec": f"{timestamp_col}_sec"})
    
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


def merge_speeds_to_segment_geom(
    speeds_df: dd.DataFrame,
    segments_gdf: gpd.GeoDataFrame,
    segment_identifier_cols: list
) -> dg.GeoDataFrame:
    """
    Merge in segment geometry
    """
    speed_with_segment_geom = dd.merge(
        segments_gdf,
        speeds_df,
        on = segment_identifier_cols,
        how = "inner",
    )
    
    return speed_with_segment_geom