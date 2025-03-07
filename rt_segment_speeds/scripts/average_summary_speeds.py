"""
Quick aggregation for summary speeds.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from typing import Literal

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import (gtfs_schedule_wrangling, 
                                 metrics,
                                 )
from shared_utils import time_helpers
from segment_speed_utils.project_vars import SEGMENT_TYPES
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT


def merge_in_common_shape_geometry(
    speeds: pd.DataFrame,
    analysis_date_list: list,
) -> gpd.GeoDataFrame:
    """
    Import the shape geometry. Since route-direction can have many 
    shape combinations, we'll use the shape that had the most trips
    to represent average speeds for that route-direction.
    """
    common_shape_geom = pd.concat(
        [gtfs_schedule_wrangling.most_common_shape_by_route_direction(
            analysis_date).to_crs(WGS84) 
            for analysis_date in analysis_date_list
        ], axis=0, ignore_index=True
    ).drop_duplicates().reset_index(drop=True)
        
    col_order = [c for c in speeds.columns]

    # The merge columns list should be all the columns that are in common
    geom_file_cols = common_shape_geom.columns.tolist()
    merge_cols = list(set(col_order).intersection(geom_file_cols))
    
    speeds_with_geom = pd.merge(
        common_shape_geom,
        speeds,
        on = merge_cols,
        how = "inner"
    ).reset_index(drop=True).reindex(
        columns = col_order + ["route_name", "geometry"]
    )
    
    return speeds_with_geom


def trip_summary_speeds_by_time_of_day(
    analysis_date: str, 
    segment_type: Literal["rt_stop_times"],
):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """
    start = datetime.datetime.now()

    dict_inputs = GTFS_DATA_DICT[segment_type]
    
    INPUT_FILE = dict_inputs.stage4
    TRIP_FILE = dict_inputs["trip_speeds_single_summary"]
        
    group_cols = [
        "schedule_gtfs_dataset_key",
        "route_id", "direction_id",
    ]
    
    df = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
    ).dropna(subset="speed_mph").pipe(
        metrics.weighted_average_speeds_across_segments,
        group_cols + ["time_of_day"]
    )
    
    trip_avg = compute(df)[0]
    trip_avg.to_parquet(
        f"{SEGMENT_GCS}{TRIP_FILE}_{analysis_date}.parquet"
    )
    
    end = datetime.datetime.now()
    
    logger.info(
        f"{segment_type} summary speed averaging by time-of-day {analysis_date} "
        f"execution time: {end - start}"
    )
    
    return
    
    
def summary_speeds_by_peak_offpeak(
    analysis_date: str, 
    segment_type: Literal["rt_stop_times"],
):   
    """
    Calculate route-direction peak/offpeak summary speeds 
    using weighted averages starting with 
    time-of-day summary speeds by route-direction.
    """
    start = datetime.datetime.now()

    dict_inputs = GTFS_DATA_DICT[segment_type]
    
    INPUT_FILE = dict_inputs.trip_speeds_single_summary
    EXPORT_FILE = dict_inputs.route_dir_timeofday
    
    CROSSWALK_COLS = [*dict_inputs["crosswalk_cols"]]
    ROUTE_DIR_COLS = [*dict_inputs["route_dir_cols"]]
    METERS_CUTOFF = dict_inputs["min_meters_elapsed"]
    MAX_TRIP_SECONDS = dict_inputs["max_trip_minutes"] * 60
    MIN_TRIP_SECONDS = dict_inputs["min_trip_minutes"] * 60
    
    # Import trips that meet minimum thresholds for trip length 
    # in distance traveled and seconds elapsed
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
        filters = [[
                ("meters_elapsed", ">=", METERS_CUTOFF), 
                ("sec_elapsed", ">=", MIN_TRIP_SECONDS),
                ("sec_elapsed", "<=", MAX_TRIP_SECONDS)
            ]] 
    ).pipe(gtfs_schedule_wrangling.add_peak_offpeak_column)

    avg_speeds = delayed(metrics.concatenate_peak_offpeak_allday_averages)(
        df,
        group_cols = ["schedule_gtfs_dataset_key"] + ROUTE_DIR_COLS,
        metric_type = "summary_speeds"
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        [analysis_date],
        columns = CROSSWALK_COLS
    ).reset_index(drop=True)
    
    avg_speeds_with_geom = delayed(merge_in_common_shape_geometry)(
        avg_speeds,
        [analysis_date]
    )
        
    avg_speeds_with_geom = compute(avg_speeds_with_geom)[0]
    
    utils.geoparquet_gcs_export(
        avg_speeds_with_geom,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    
    logger.info(
        f"{segment_type} summary speed averaging by peak/offpeak for {analysis_date} "
        f"execution time: {end - start}"
    )
    
    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    from shared_utils import rt_dates
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    segment_type = "rt_stop_times"
        
    for analysis_date in analysis_date_list:
              
        trip_summary_speeds_by_time_of_day(
            analysis_date, 
            segment_type,
        )
        
        summary_speeds_by_peak_offpeak(
            analysis_date,
            segment_type
        )