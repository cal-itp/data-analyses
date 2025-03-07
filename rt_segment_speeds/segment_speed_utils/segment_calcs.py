"""
Functions related to calculating segment speeds.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import numpy as np
import pandas as pd

from numba import jit
from typing import Literal, Union

from shared_utils import dask_utils
from calitp_data_analysis.geography_utils import WGS84
from shared_utils.rt_utils import MPH_PER_MPS
from segment_speed_utils.project_vars import GTFS_DATA_DICT, SEGMENT_TYPES

def speed_from_meters_elapsed_sec_elapsed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert total meters elapsed and seconds elapsed 
    into miles per hour.
    """
    df = df.assign(
        speed_mph = (df.meters_elapsed.divide(df.sec_elapsed) * 
                     MPH_PER_MPS)
    )
    return df


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
    
    df = speed_from_meters_elapsed_sec_elapsed(df)
    
    return df


def calculate_avg_speeds(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Calculate the median, 20th, and 80th percentile speeds 
    by groups.
    """
    # pd.groupby and pd.quantile is so slow
    # create our own list of speeds and use np
    df2 = (df.groupby(group_cols, 
                      observed=True, group_keys=False, dropna=False)
           .agg({"speed_mph": lambda x: sorted(list(x))})
           .reset_index()
           .rename(columns = {"speed_mph": "speed_mph_list"}))
                        
    df2 = df2.assign(
        p50_mph = df2.apply(lambda x: np.percentile(x.speed_mph_list, q=50), axis=1),
        n_trips = df2.apply(lambda x: len(x.speed_mph_list), axis=1).astype("int16"),
        p20_mph = df2.apply(lambda x: np.percentile(x.speed_mph_list, q=20), axis=1),
        p80_mph = df2.apply(lambda x: np.percentile(x.speed_mph_list, q=80), axis=1),
    )
    
    stats = df2.drop(columns = "speed_mph_list")
    
    # Clean up for map
    speed_cols = [c for c in stats.columns if "_mph" in c]
    stats[speed_cols] = stats[speed_cols].round(2)
    
    return stats

                                   
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


def interpolate_stop_arrival_time(
    stop_position: float, 
    shape_meters_arr: np.ndarray,
    timestamp_arr: np.ndarray
) -> float:
    """
    Interpolate the arrival time given the stop meters position.
    Cast datetimes into floats and cast back as datetime.
    """
    timestamp_arr = np.asarray(timestamp_arr).astype("datetime64[s]").astype("float64")

    return np.interp(
        stop_position, np.asarray(shape_meters_arr), timestamp_arr
    ).astype("datetime64[s]")


@jit(nopython=True)
def monotonic_check(arr: np.ndarray) -> bool:
    """
    For an array, check if it's monotonically increasing. 
    https://stackoverflow.com/questions/4983258/check-list-monotonicity
    """
    diff_arr = np.diff(arr)
    
    if np.all(diff_arr > 0):
        return True
    else:
        return False
    
def rolling_window_make_array(
    df: pd.DataFrame, 
    window: int, 
    rolling_col: str
) -> pd.DataFrame:
    """
    Interpolated stop arrival times are checked
    to see if they are monotonically increasing.
    If it isn't, it gets recalculated based on 
    stop_meters (the stop's position) relative to
    other stop arrival times.
    
    https://stackoverflow.com/questions/47482009/pandas-rolling-window-to-return-an-array
    """
    df[f"rolling_{rolling_col}"] = [
        np.asarray(window) for window in 
        df.groupby("trip_instance_key")[rolling_col].rolling(
            window = window, center=True)
    ]
    
    is_monotonic_series = np.vectorize(monotonic_check)(df[f"rolling_{rolling_col}"])
    df[f"{rolling_col}_monotonic"] = is_monotonic_series
    
    return df


def merge_in_segment_geometry(
    speeds_by_segment: pd.DataFrame,
    analysis_date_list: list,
    segment_type: Literal[SEGMENT_TYPES],
    segment_cols: list
) -> gpd.GeoDataFrame:
    """
    Import the segments to merge and attach it to the average speeds.
    """
    dict_inputs = GTFS_DATA_DICT[segment_type]
    SEGMENT_FILE = dict_inputs.segments_file
    
    GCS_PATH = dict_inputs.dir
    
    paths = [f"{GCS_PATH}{SEGMENT_FILE}" for date in analysis_date_list]

    segment_geom = dask_utils.get_ddf(
        paths, 
        analysis_date_list, 
        data_type = "gdf",
        get_pandas = False,
        add_date = False, 
        columns = segment_cols
    ).drop_duplicates().to_crs(WGS84).compute()   
    
    col_order = [c for c in speeds_by_segment.columns]
    
    # The merge columns list should be all the columns that are in common
    # between averaged speeds and segment gdf
    geom_file_cols = segment_geom.columns.tolist()
    merge_cols = list(set(col_order).intersection(geom_file_cols))
    
    gdf = pd.merge(
        segment_geom[merge_cols + ["geometry"]].drop_duplicates(),
        speeds_by_segment,
        on = merge_cols, 
    ).reset_index(drop=True).reindex(
        columns = col_order + ["geometry"]
    )
    
    return gdf


def calculate_weighted_averages(
    df: pd.DataFrame, 
    group_cols: list, 
    metric_cols: list, 
    weight_col: str
):
    """
    For certain aggregations, we need to calculate a weighted average, 
    weighted by the number of trips.
    
    If we want peak/offpeak weighted calculations, 
    we can take time-of-day (AM peak, PM peak) and
    get a peak speed calculation, after weighting by the number
    of trips present in each time-of-day bin.
    
    Ex: metric_cols = ['p20_mph', 'p50_mph', 'p80_mph']
    weight_cols = 'n_trips'
    
    """    
    for c in metric_cols:
        df[c] = df[c] * df[weight_col]    
    
    df2 = (df.groupby(group_cols, group_keys=False)
           .agg({c: "sum" for c in metric_cols + [weight_col]})
           .reset_index()
          )
    
    for c in metric_cols:
        df2[c] = df2[c].divide(df2[weight_col]).round(2)
    
    return df2
