"""
Quick aggregation for summary speeds.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from pathlib import Path
from typing import Literal

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import (gtfs_schedule_wrangling, 
                                 helpers, 
                                 metrics,
                                 segment_calcs,
                                 time_helpers, 
                                 time_series_utils
                                 )
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH
from segment_speed_utils.time_series_utils import ROUTE_DIR_COLS

OPERATOR_COLS = [
    "schedule_gtfs_dataset_key", 
]    

def single_day_summary_averages(analysis_date: str, dict_inputs: dict):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    
    TRIP_FILE = dict_inputs["trip_speeds_single_summary"]
    ROUTE_DIR_FILE = dict_inputs["route_dir_single_summary"]
    
    METERS_CUTOFF = dict_inputs["min_meters_elapsed"]
    MAX_TRIP_SECONDS = dict_inputs["max_trip_minutes"] * 60
    MIN_TRIP_SECONDS = dict_inputs["min_trip_minutes"] * 60
    
    start = datetime.datetime.now()
    
    trip_group_cols = OPERATOR_COLS + ROUTE_DIR_COLS + [
        "shape_array_key", "shape_id",
        "trip_instance_key",
        "time_of_day"
    ]
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        SPEED_FILE,
        [analysis_date],
        data_type  = "df",
        get_pandas = True,
        columns = trip_group_cols + ["meters_elapsed", "sec_elapsed"],
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    )
    print("concatenated files") 
    
    trip_avg = metrics.weighted_average_speeds_across_segments(
        df,
        trip_group_cols + ["peak_offpeak"],
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        [analysis_date]
    ).reset_index(drop=True)
    
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
    
    route_dir_avg = metrics.concatenate_peak_offpeak_allday_averages(
        trip_avg_filtered,
        OPERATOR_COLS + ROUTE_DIR_COLS,
        metric_type = "summary_speeds"
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        [analysis_date]
    ).reset_index(drop=True)
    
    col_order = [c for c in route_dir_avg.columns]
    
    common_shape_geom = gtfs_schedule_wrangling.most_common_shape_by_route_direction(
        analysis_date).to_crs(WGS84)
    
    route_dir_avg = pd.merge(
        common_shape_geom,
        route_dir_avg,
        on = OPERATOR_COLS + ROUTE_DIR_COLS,
        how = "inner"
    ).reset_index(drop=True).reindex(
        columns = col_order + ["route_name", "geometry"]
    )
    
    utils.geoparquet_gcs_export(
        route_dir_avg,
        SEGMENT_GCS,
        f"{ROUTE_DIR_FILE}_{analysis_date}"
    )
    
    del route_dir_avg, common_shape_geom
    
    time2 = datetime.datetime.now()
    logger.info(f"route dir avg: {time2 - time1}")
    logger.info(f"single day summary speed execution time: {time2 - start}")
    
    return


def multi_day_summary_averages(analysis_date_list: list, dict_inputs: dict):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    The main difference from a single day average is that
    the seven days is concatenated first before averaging,
    so that we get weighted averages.
    """        
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    ROUTE_DIR_FILE = dict_inputs["route_dir_multi_summary"]
    
    METERS_CUTOFF = dict_inputs["min_meters_elapsed"]
    MAX_TRIP_SECONDS = dict_inputs["max_trip_minutes"] * 60
    MIN_TRIP_SECONDS = dict_inputs["min_trip_minutes"] * 60
    
    start = datetime.datetime.now()
    
    trip_group_cols = OPERATOR_COLS + ROUTE_DIR_COLS + [
        "trip_instance_key",
        "time_of_day"
    ]
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        SPEED_FILE,
        analysis_date_list,
        data_type  = "df",
        get_pandas = False,
        columns = trip_group_cols + ["meters_elapsed", "sec_elapsed"],
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    )
    print("concatenated files") 
    
    trip_avg = delayed(metrics.weighted_average_speeds_across_segments)(
        df,
        trip_group_cols + ["peak_offpeak", "weekday_weekend"],
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        analysis_date_list
    ).reset_index(drop=True)

    trip_avg_filtered = trip_avg[
        (trip_avg.meters_elapsed >= METERS_CUTOFF) & 
        (trip_avg.sec_elapsed >= MIN_TRIP_SECONDS) & 
        (trip_avg.sec_elapsed <= MAX_TRIP_SECONDS)
    ]
    
    time_span_str, time_span_num = time_helpers.time_span_labeling(
        analysis_date_list)
    
    route_dir_avg = delayed(metrics.concatenate_peak_offpeak_allday_averages)(
        trip_avg_filtered,
        OPERATOR_COLS + ROUTE_DIR_COLS + ["weekday_weekend"],
        metric_type = "summary_speeds"
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        analysis_date_list
    ).reset_index(drop=True)
    
    route_dir_avg = compute(route_dir_avg)[0]
    
    route_dir_avg = time_helpers.add_time_span_columns(
        route_dir_avg, time_span_num
    )
    
    col_order = [c for c in route_dir_avg.columns]
    
    # Use Wednesday to select a shape
    common_shape_geom = gtfs_schedule_wrangling.most_common_shape_by_route_direction(
        analysis_date_list[2]).to_crs(WGS84)
    
    route_dir_avg = pd.merge(
        common_shape_geom,
        route_dir_avg,
        on = OPERATOR_COLS + ROUTE_DIR_COLS,
        how = "inner"
    ).reset_index(drop=True).reindex(
        columns = col_order + ["route_name", "geometry"]
    )
    
    utils.geoparquet_gcs_export(
        route_dir_avg,
        SEGMENT_GCS,
        f"{ROUTE_DIR_FILE}_{time_span_str}"
    )
    
    end = datetime.datetime.now()
    logger.info(f"multi day summary speed execution time: {end - start}")
    
    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    from shared_utils import rt_dates
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    dict_inputs = helpers.get_parameters(CONFIG_PATH, "rt_stop_times")
    
    
    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()
        
        single_day_summary_averages(analysis_date, dict_inputs)
        end = datetime.datetime.now()
        
        logger.info(f"average rollups for {analysis_date}: {end - start}")
        
    '''
    for one_week in [rt_dates.oct_week, rt_dates.apr_week]:
        start = datetime.datetime.now()
            
        multi_day_summary_averages(one_week, dict_inputs)
        end = datetime.datetime.now()
    
        logger.info(f"average rollups for {one_week}: {end - start}")
    '''