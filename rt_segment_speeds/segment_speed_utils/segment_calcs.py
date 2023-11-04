import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import numpy as np
import pandas as pd

from typing import Union

from shared_utils import rt_utils


def derive_speed(
    df: pd.DataFrame, 
    distance_cols: tuple = ("prior_shape_meters", "shape_meters"), 
    time_cols: tuple = ("prior_location_timestamp_local_sec", 
                        "location_timestamp_local_sec")
) -> pd.DataFrame:
    """
    Derive meters and sec elapsed to calculate speed_mph.
    """
    min_dist, max_dist = distance_cols[0], distance_cols[1]
    min_time, max_time = time_cols[0], time_cols[1]    
    
    df = df.assign(
        meters_elapsed = (df[max_dist] - df[min_dist]).abs()
    )
    
    if df[min_time].dtype in ["float", "int"]:
        # If 2 time cols are already converted to seconds, just take difference
        df = df.assign(
            sec_elapsed = (df[max_time] - df[min_time]).abs()
        )
    else:
        # If 2 time cols are datetime, convert timedelta to seconds
        df = df.assign(
            sec_elapsed = (df[max_time] - df[min_time]).divide(
                           np.timedelta64(1, 's')).abs(),
        )
    
    df = df.assign(
        speed_mph = (df.meters_elapsed.divide(df.sec_elapsed) * 
                     rt_utils.MPH_PER_MPS)
    )
    
    return df

                                   
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


def get_usable_vp_bounds_by_trip(df: dd.DataFrame) -> pd.DataFrame:
    """
    Of all the usable vp, for each trip, find the min(vp_idx)
    and max(vp_idx).
    For the first stop, there will never be a previous vp to find,
    because the previous vp_idx will belong to a different operator/trip.
    But for segments in the middle of the shape, the previous vp can be anywhere,
    maybe several segments away.
    """
    if isinstance(df, pd.DataFrame):
        df = dd.from_pandas(df, npartitions=3)
        
    grouped_df = df.groupby("trip_instance_key", 
                            observed=True, group_keys=False)

    start_vp = (grouped_df.vp_idx.min().reset_index()
                .rename(columns = {"vp_idx": "min_vp_idx"})
               )
    end_vp = (grouped_df.vp_idx.max().reset_index()
              .rename(columns = {"vp_idx": "max_vp_idx"})
             )
    
    df2 = dd.merge(
        start_vp,
        end_vp,
        on = "trip_instance_key",
        how = "left"
    ).reset_index(drop=True).compute()
    
    return df2
