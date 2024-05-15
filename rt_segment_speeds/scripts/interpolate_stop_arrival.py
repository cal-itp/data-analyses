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

from segment_speed_utils import (array_utils, helpers,
                                 segment_calcs, wrangle_shapes)
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import PROJECT_CRS, SEGMENT_TYPES
                                             

def project_points_onto_shape(
    stop_geometry: shapely.Point,
    vp_coords_trio: shapely.LineString,
    shape_geometry: shapely.LineString,
    timestamp_arr: np.ndarray,
) -> tuple[float]:
    """
    Project the points in the vp trio against the shape geometry
    and the stop position onto the shape geometry.
    Use np.interp to find interpolated arrival time
    """
    stop_position = vp_coords_trio.project(stop_geometry)
    stop_meters = shape_geometry.project(stop_geometry)
    
    points = [shapely.Point(p) for p in vp_coords_trio.coords]
    projected_points = np.asarray([vp_coords_trio.project(p) for p in points])

    interpolated_arrival = wrangle_shapes.interpolate_stop_arrival_time(
        stop_position, projected_points, timestamp_arr)
        
    return stop_meters, interpolated_arrival


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
    df: pd.DataFrame
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
        ["trip_instance_key", "stop_sequence"]
    ).reset_index(drop=True)
         
    del no_fix, fix1, df
        
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

    NEAREST_VP = f"{dict_inputs['stage2']}_{analysis_date}"
    STOP_ARRIVALS_FILE = f"{dict_inputs['stage3']}_{analysis_date}"
    
    start = datetime.datetime.now()
    
    # Import nearest vp file, set to EPSG:3310 
    # to merge against scheduled shapes
    df = gpd.read_parquet(
        f"{SEGMENT_GCS}{NEAREST_VP}.parquet",
    )

    df = df.assign(
        stop_geometry = df.stop_geometry.to_crs(PROJECT_CRS),
        vp_coords_trio = df.vp_coords_trio.to_crs(PROJECT_CRS)
    )

    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS
    ).dropna(subset="geometry")

    gdf = pd.merge(
        df,
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    )

    del df, shapes

    stop_meters_series = []
    stop_arrival_series = []
    
    for row in gdf.itertuples():
        
        stop_meters, interpolated_arrival = project_points_onto_shape(
            getattr(row, "stop_geometry"),
            getattr(row, "vp_coords_trio"),
            getattr(row, "shape_geometry"),
            getattr(row, "location_timestamp_local_trio")
        )
        
        stop_meters_series.append(stop_meters)
        stop_arrival_series.append(interpolated_arrival)

    results = gdf.assign(
        stop_meters = stop_meters_series,
        arrival_time = stop_arrival_series,
    )[["trip_instance_key", "shape_array_key",
         "stop_sequence", "stop_id", 
         "stop_meters", "arrival_time"]
     ].sort_values(
        ["trip_instance_key", "stop_sequence"]
    ).reset_index(drop=True)
    
    time1 = datetime.datetime.now()
    logger.info(f"get stop arrivals {analysis_date}: {time1 - start}")
    
    results = enforce_monotonicity_and_interpolate_across_stops(results)
        
    results.to_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}.parquet"
    )
    
    del gdf, results
    
    end = datetime.datetime.now()
    logger.info(f"interpolate arrivals for {segment_type} "
                f"{analysis_date}:  {analysis_date}: {end - start}")    
    
    return


if __name__ == "__main__":
        
    LOG_FILE = "../logs/interpolate_stop_arrival.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    from segment_speed_utils.project_vars import analysis_date_list
    
    for analysis_date in analysis_date_list:
        interpolate_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )