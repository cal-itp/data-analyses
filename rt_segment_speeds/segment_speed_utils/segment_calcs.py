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
    timestamp_col: str = "location_timestamp"
) -> dd.DataFrame:
    """
    For each segment-trip combination, throw away excess points, just 
    keep the enter/exit points for the segment.
    """
    segment_trip_cols = ["gtfs_dataset_key", 
                         "trip_id"] + segment_identifier_cols
    
    # https://stackoverflow.com/questions/52552066/dask-compute-gives-attributeerror-series-object-has-no-attribute-encode    
    # comment out .compute() and just .reset_index()
    enter = (vp_to_seg.groupby(segment_trip_cols)
             [timestamp_col].min()
             #.compute()
             .reset_index()
            )

    exit = (vp_to_seg.groupby(segment_trip_cols)
            [timestamp_col].max()
            #.compute()
            .reset_index()
           )
    
    enter_exit = dd.multi.concat([enter, exit], axis=0)
    
    # Merge back in with original to only keep the min/max timestamps
    # dask can't sort by multiple columns to drop
    enter_exit_full_info = dd.merge(
        vp_to_seg,
        enter_exit,
        on = segment_trip_cols + [timestamp_col],
        how = "inner"
    ).reset_index(drop=True)
        
    return enter_exit_full_info


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
    gdf: dg.GeoDataFrame, 
    segment_identifier_cols: list,
    timestamp_col: str = "location_timestamp"
) -> dd.DataFrame:
    """
    For each segment-trip pair, calculate find the min/max timestamp
    and min/max shape_meters. Use that to derive speed column.
    """ 
    segment_trip_cols = [
        "gtfs_dataset_key", "_gtfs_dataset_name", 
        "trip_id"] + segment_identifier_cols
    
    min_time = (gdf.groupby(segment_trip_cols)
                [timestamp_col].min()
                .compute()
                .reset_index(name="min_time")
    )
    
    max_time = (gdf.groupby(segment_trip_cols)
                [timestamp_col].max()
                .compute()
                .reset_index(name="max_time")
               )
    
    min_dist = (gdf.groupby(segment_trip_cols)
                .shape_meters.min()
                .compute()
                .reset_index(name="min_dist")
    )
    
    max_dist = (gdf.groupby(segment_trip_cols)
                .shape_meters.max()
                .compute()
                .reset_index(name="max_dist")
               )  
    
    base_agg = gdf[segment_trip_cols].drop_duplicates().reset_index(drop=True)
    
    segment_trip_agg = (
        base_agg
        .merge(min_time, on = segment_trip_cols, how = "left")
        .merge(max_time, on = segment_trip_cols, how = "left")
        .merge(min_dist, on = segment_trip_cols, how = "left")
        .merge(max_dist, on = segment_trip_cols, how = "left")
    )
    
    segment_speeds = derive_speed(
        segment_trip_agg,
        distance_cols = ("min_dist", "max_dist"),
        time_cols = ("min_time", "max_time")
    )
        
    return segment_speeds