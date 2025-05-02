"""
Use projected stop times, calculate speeds
on the segment between each stop.
"""
import datetime
import numpy as np
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT
from shared_utils import rt_dates
import model_utils

def set_intervaled_distance_cutoffs(
    start_dist: int, 
    end_dist: int, 
    meters_interval: int = 100
) -> np.ndarray:
    """
    General use of getting an array of custom cutoffs to use.
    For now, we can use every 100 meters.
    This will take the start, end of a trip's vp interpolated distances
    and return an array with like [start, start + 100, start + 200, ..., end]
    """
    return np.array(range(start_dist, end_dist, meters_interval))


def speed_arrays_by_segment_trip(
    df: pd.DataFrame, 
    trip_group_cols: list = ["trip_instance_key"],
    resampled_timestamp_col: str = "resampled_timestamps", 
    interpolated_distance_col: str = "interpolated_distances", 
    meters_interval: int = None,
    cutoffs_col: str = None
) -> pd.DataFrame:
    """
    For each trip, use the cutoffs column that defines where the segment cutoffs are,
    and calculate change in seconds, meters, and speed for each segment.
    """
    intervaled_cutoffs = []
    meters_series = []
    seconds_series = []
    speed_series = []
    
    for row in df.itertuples():
        
        one_trip_distance_arr = getattr(row, interpolated_distance_col)
        one_trip_timestamp_arr = getattr(row, resampled_timestamp_col)
        
        start_dist = int(np.floor(one_trip_distance_arr).min())
        end_dist = int(np.ceil(one_trip_distance_arr).max())
        
        # If we have our own stop times (stop positions condensed as an array), use that column here
        if cutoffs_col is not None:
            intervaled_distance_cutoffs = getattr(row, cutoffs_col)
        
        # If there's the need of a generalized cutoff, like, take vp and calculate every 100 meters, 250 meters, etc,
        # use this
        if meters_interval is not None:
            # Don't allow inputs of floats
            meters_interval = int(round(meters_interval))
            intervaled_distance_cutoffs = set_intervaled_distance_cutoffs(start_dist, end_dist, meters_interval)
        
        
        trip_delta_meters, trip_delta_seconds, trip_speeds = model_utils.calculate_meters_seconds_speed_deltas(
            one_trip_distance_arr, 
            one_trip_timestamp_arr,
            intervaled_distance_cutoffs,
        )
        
        intervaled_cutoffs.append(intervaled_distance_cutoffs)
        meters_series.append(trip_delta_meters)
        seconds_series.append(trip_delta_seconds)
        speed_series.append(trip_speeds)
        
        
    # Assign series as columns
    df2 = df.assign(
        meters_elapsed = meters_series,
        seconds_elapsed = seconds_series,
        speed_mph = speed_series    
    )
    
    modeled_cols = [
        "meters_elapsed", "seconds_elapsed", "speed_mph"
    ]
    
    keep_cols = trip_group_cols + modeled_cols
    
    # Only where we use general 100 meters calculate, then we want to return than extra column
    if meters_interval is not None:    
        df2 = df2.assign(
            intervaled_meters = intervaled_cutoffs,
        )[keep_cols + ["intervaled_meters"]]
    
    # In general, define the columns we want to keep with trip_group
    else:
        df2 = df2[keep_cols]    
        
    return df2


if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_speeds.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    analysis_date_list = [
        rt_dates.DATES["oct2024"]
    ]    

    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()

        INPUT_FILE = GTFS_DATA_DICT.modeled_vp.resampled_vp
        STOP_TIMES_FILE = GTFS_DATA_DICT.modeled_rt_stop_times.stop_times_projected
        EXPORT_FILE = GTFS_DATA_DICT.modeled_rt_stop_times.speeds_wide

        df = pd.read_parquet(
            f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet"
        )

        subset_trips = df.trip_instance_key.unique().tolist()

        stop_time_cutoffs = pd.read_parquet(
            f"{SEGMENT_GCS}{STOP_TIMES_FILE}_{analysis_date}.parquet",
            filters = [[("trip_instance_key", "in", subset_trips)]],
            columns = ["trip_instance_key", "stop_sequence", 
                       "stop_meters"] 
            # should stop_meters_increasing be used here?
        )

        gdf = pd.merge(
            df,
            stop_time_cutoffs,
            on = "trip_instance_key",
            how = "inner"
        )

        
        results = speed_arrays_by_segment_trip(
            gdf,
            trip_group_cols = ["trip_instance_key", "stop_sequence"], 
            cutoffs_col = "stop_meters",
        )

        results.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
        )

        end = datetime.datetime.now()
        logger.info(f"{analysis_date}: speeds every stop: {end - start}")