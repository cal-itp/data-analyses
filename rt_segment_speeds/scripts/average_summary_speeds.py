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
                                 time_helpers, 
                                 )
from segment_speed_utils.project_vars import SEGMENT_TYPES
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from average_segment_speeds import (OPERATOR_COLS, CROSSWALK_COLS, 
                                    concatenate_trip_segment_speeds)


def merge_in_common_shape_geometry(
    speeds: pd.DataFrame,
    analysis_date: str,
) -> gpd.GeoDataFrame:
    """
    Import the shape geometry. Since route-direction can have many 
    shape combinations, we'll use the shape that had the most trips
    to represent average speeds for that route-direction.
    
    For a week's worth of data, we'll just use Wed shapes.
    """
    # Use Wednesday to select a shape
    common_shape_geom = gtfs_schedule_wrangling.most_common_shape_by_route_direction(
        analysis_date
    ).to_crs(WGS84)
        
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


def summary_average_speeds(
    analysis_date_list: list, 
    segment_type: Literal[SEGMENT_TYPES],
    group_cols: list,
    export_file: str
):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """
    dict_inputs = GTFS_DATA_DICT[segment_type]
    
    TRIP_FILE = dict_inputs["trip_speeds_single_summary"]
    
    METERS_CUTOFF = dict_inputs["min_meters_elapsed"]
    MAX_TRIP_SECONDS = dict_inputs["max_trip_minutes"] * 60
    MIN_TRIP_SECONDS = dict_inputs["min_trip_minutes"] * 60
    
    start = datetime.datetime.now()
    
    trip_group_cols = group_cols + [
        "shape_array_key", "shape_id",
        "trip_instance_key",
        "time_of_day"
    ]
    
    df = concatenate_trip_segment_speeds(
        analysis_date_list,
        segment_type
    )
    
    trip_avg = metrics.weighted_average_speeds_across_segments(
        df,
        trip_group_cols + ["peak_offpeak"],
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        analysis_date_list,
        columns = CROSSWALK_COLS
    ).reset_index(drop=True)
    
    
    if len(analysis_date_list) > 1:
        # If a week (date list) is put in, use Wednesday for segment geometry
        time_span_str, _ = time_helpers.time_span_labeling(
            analysis_date_list)
        
        analysis_date = analysis_date_list[2]
    
    else:
        # If a single day is put in, use that date for segment geometry
        analysis_date = analysis_date_list[0]
        time_span_str = analysis_date
        
        trip_avg = compute(trip_avg)[0]
        
        trip_avg.to_parquet(
            f"{SEGMENT_GCS}{TRIP_FILE}_{analysis_date}.parquet"
        )
    
        time1 = datetime.datetime.now()
        logger.info(f"trip avg {time1 - start}")
    
    
    trip_avg_filtered = trip_avg[
        (trip_avg.meters_elapsed >= METERS_CUTOFF) & 
        (trip_avg.sec_elapsed >= MIN_TRIP_SECONDS) & 
        (trip_avg.sec_elapsed <= MAX_TRIP_SECONDS)
    ]
    
    avg_speeds = delayed(metrics.concatenate_peak_offpeak_allday_averages)(
        trip_avg_filtered,
        group_cols,
        metric_type = "summary_speeds"
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        analysis_date_list,
        columns = CROSSWALK_COLS
    ).reset_index(drop=True)
    
    avg_speeds_with_geom = delayed(merge_in_common_shape_geometry)(
        avg_speeds,
        analysis_date
    )
        
    avg_speeds_with_geom = compute(avg_speeds_with_geom)[0]
    
    utils.geoparquet_gcs_export(
        avg_speeds_with_geom,
        SEGMENT_GCS,
        f"{export_file}_{time_span_str}"
    )
    
    
    end = datetime.datetime.now()
    logger.info(f"summary speed averaging for {analysis_date_list} execution time: {end - start}")
    
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
    
    dict_inputs = GTFS_DATA_DICT[segment_type]
    ROUTE_DIR_COLS = [*dict_inputs["route_dir_cols"]]
    
    ROUTE_DIR_FILE = dict_inputs["route_dir_single_summary"]
    
    for analysis_date in analysis_date_list:
              
        summary_average_speeds(
            [analysis_date], 
            segment_type,
            group_cols = OPERATOR_COLS + ROUTE_DIR_COLS,
            export_file = ROUTE_DIR_FILE
        )
        
    '''
    from segment_speed_utils.project_vars import weeks_available
    
    ROUTE_DIR_FILE = dict_inputs["route_dir_multi_summary"]

    for one_week in weeks_available:
            
        summary_average_speeds(
            one_week, 
            segment_type,
            group_cols = OPERATOR_COLS + ROUTE_DIR_COLS + ["weekday_weekend"],
            export_file = ROUTE_DIR_FILE
        )    
    '''