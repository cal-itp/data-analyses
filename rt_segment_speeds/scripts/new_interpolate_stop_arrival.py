import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from segment_speed_utils import helpers, wrangle_shapes
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import PROJECT_CRS, SEGMENT_TYPES
from shared_utils import rt_dates
import interpolate_stop_arrival
import new_vp_around_stops as nvp


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
        trip_stop_cols    
    )
    
    results = interpolate_stop_arrival.enforce_monotonicity_and_interpolate_across_stops(
        df, trip_stop_cols + ["stop_meters"])
    
        
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
