import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from segment_speed_utils import (array_utils, helpers, 
                                 segment_calcs, wrangle_shapes)
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import PROJECT_CRS, SEGMENT_TYPES
from shared_utils import rt_dates
#import interpolate_stop_arrival
import vp_around_stops as nvp


def add_arrival_time(
    input_file: str,
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
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{input_file}_{analysis_date}.parquet"
    ).pipe(nvp.consolidate_surrounding_vp, group_cols)
    
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
    
    return df2


def stop_and_arrival_time_arrays_by_trip(
    df: pd.DataFrame, 
) -> pd.DataFrame:
    """
    For stops that violated the monotonically increasing condition,
    set those arrival_times to NaT again.
    Now, look across stops and interpolate again, using stop_meters.
    """
    # Add columns with the trip's stop_meters and arrival_times
    # for only correctly interpolated values
    df_arrays = (df[df.arrival_time.notna()]
           .groupby("trip_instance_key")
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
    fix1 = stop_and_arrival_time_arrays_by_trip(fix1)
    
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
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    dict_inputs = config_path[segment_type]
    trip_stop_cols = [*dict_inputs["trip_stop_cols"]]
    INPUT_FILE = dict_inputs["stage2b"]
    STOP_ARRIVALS_FILE = dict_inputs["stage3"]

    start = datetime.datetime.now()
    
    df = add_arrival_time(
        INPUT_FILE, 
        analysis_date,
        trip_stop_cols + ["shape_array_key"]   
    )
    
    results = enforce_monotonicity_and_interpolate_across_stops(
        df, trip_stop_cols)
    
        
    results.to_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}_{analysis_date}.parquet"
    )
    
    end = datetime.datetime.now()
    logger.info(f"interpolate arrivals for {segment_type} "
                f"{analysis_date}:  {analysis_date}: {end - start}") 


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    for analysis_date in analysis_date_list:
        interpolate_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )
