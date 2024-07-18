"""
Interpolate stop arrival.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from segment_speed_utils import (helpers,
                                 segment_calcs, wrangle_shapes)
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import PROJECT_CRS, SEGMENT_TYPES


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
    config_path: Optional[Path] = GTFS_DATA_DICT,    
):
    """
    Find the interpolated stop arrival based on where 
    stop is relative to the trio of vehicle positions.
    """
    dict_inputs = config_path[segment_type]

    NEAREST_VP = dict_inputs['stage2b']
    STOP_ARRIVALS_FILE = dict_inputs['stage3']
    
    trip_stop_cols = [*dict_inputs["trip_stop_cols"]]
    
    start = datetime.datetime.now()
    
    gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}{NEAREST_VP}_{analysis_date}.parquet"
    )
    
    arrival_time_series = []
    
    for row in gdf.itertuples():
        
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
        
    gdf["arrival_time"] = arrival_time_series
    
    drop_cols = [i for i in gdf.columns if 
                 ("prior_" in i) or ("subseq_" in i)]
    
    gdf2 = gdf.drop(columns = drop_cols + ["stop_geometry"])
    
    time1 = datetime.datetime.now()
    logger.info(f"get stop arrivals {analysis_date}: {time1 - start}")
    
    results = enforce_monotonicity_and_interpolate_across_stops(
        results, trip_stop_cols)
        
    results.to_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}_{analysis_date}.parquet"
    )
    
    del gdf, results
    
    end = datetime.datetime.now()
    logger.info(f"interpolate arrivals for {segment_type} "
                f"{analysis_date}:  {analysis_date}: {end - start}")    
    
    return


if __name__ == "__main__":
        
    from segment_speed_utils.project_vars import analysis_date_list
    
    for analysis_date in analysis_date_list:
        interpolate_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )
