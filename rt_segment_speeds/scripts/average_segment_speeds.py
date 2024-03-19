"""
Quick aggregation for segment speed averages.
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
                                 time_helpers, 
                                 time_series_utils
                                 )
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH
from segment_speed_utils.time_series_utils import STOP_PAIR_COLS, ROUTE_DIR_COLS

OPERATOR_COLS = [
    "schedule_gtfs_dataset_key", 
]

SHAPE_STOP_COLS = [
    "shape_array_key", "shape_id", "stop_sequence",
]


def import_segments(
    analysis_date_list: list,
    segment_type: Literal["stop_segments", "rt_stop_times"],
    get_pandas: bool
) -> gpd.GeoDataFrame:
    """
    Import the segments to merge.
    For stop_segments, import only 1 trip per shape for segments, and merge
    on shape_array_key and stop_pair.
    For rt_stop_times, import all trips with their segments, and merge on 
    trip_instance_key and stop_pair.
    """    
    keep_cols = [
        "shape_array_key", "stop_pair", 
        "schedule_gtfs_dataset_key", "route_id", "direction_id",
        "geometry"
    ]
    
    if segment_type == "stop_segments":
        SEGMENT_FILE = "segment_options/shape_stop_segments"
        
        
    elif segment_type == "rt_stop_times":
        SEGMENT_FILE = "segment_options/stop_segments"

    dfs = [
        delayed(gpd.read_parquet)(
            f"{SEGMENT_GCS}{SEGMENT_FILE}_{analysis_date}.parquet",
            columns = keep_cols,
        ).to_crs(WGS84) for analysis_date in analysis_date_list
    ]
    
    gdf = delayed(pd.concat)(
        dfs, axis=0, ignore_index=True
    ).drop_duplicates(subset=keep_cols).reset_index(drop=True)
    
    if get_pandas:
        gdf = compute(gdf)[0]
    
    return gdf


def segment_averaging_with_geometry(
    gdf: gpd.GeoDataFrame,
    group_cols: list, 
    analysis_date_list: list,
    merge_cols: list
) -> gpd.GeoDataFrame:
    """
    Calculate average speeds for segment.
    Input a list of group_cols to do the averaging over.
    Attach segment geometry.
    For a week's worth of data, we'll just use Wed segments.
    merge_cols refers to the list of columns to merge in segment geometry,
    which may slightly differ from the group_cols.
    """
    if len(analysis_date_list) > 1:
        analysis_date = analysis_date_list[2]
    else:
        analysis_date = analysis_date_list[0]
        
    avg_speeds = metrics.concatenate_peak_offpeak_allday_averages(
        gdf, 
        group_cols,
        metric_type = "segment_speeds"
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, analysis_date_list
    )
    
    col_order = [c for c in avg_speeds.columns]
    
    segment_geom = import_segments(
        [analysis_date], "stop_segments", get_pandas=True
    )[merge_cols + ["geometry"]].drop_duplicates()
        
    avg_speeds_with_geom = pd.merge(
        segment_geom,
        avg_speeds,
        on = merge_cols, 
    ).reset_index(drop=True).reindex(
        columns = col_order + ["geometry"]
    )
    
    return avg_speeds_with_geom


def single_day_segment_averages(analysis_date: str, dict_inputs: dict):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """    
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    
    SHAPE_SEG_FILE = dict_inputs["shape_stop_single_segment"]
    ROUTE_SEG_FILE = dict_inputs["route_dir_single_segment"]
        
    start = datetime.datetime.now()
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        SPEED_FILE,
        [analysis_date],
        data_type  = "df",
        get_pandas = True,
        columns = (OPERATOR_COLS + SHAPE_STOP_COLS + 
                       STOP_PAIR_COLS + ROUTE_DIR_COLS + [
                           "trip_instance_key", "speed_mph", 
                           "meters_elapsed", "sec_elapsed", 
                           "time_of_day"]),
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    )
    print("concatenated files") 
        
    shape_stop_segments = segment_averaging_with_geometry(
        df, 
        group_cols = OPERATOR_COLS + SHAPE_STOP_COLS + STOP_PAIR_COLS,
        analysis_date_list = [analysis_date],
        merge_cols = OPERATOR_COLS + ["shape_array_key"] + STOP_PAIR_COLS             
    )

    utils.geoparquet_gcs_export(
        shape_stop_segments,
        SEGMENT_GCS,
        f"{SHAPE_SEG_FILE}_{analysis_date}"
    )
        
    time1 = datetime.datetime.now()
    logger.info(f"shape seg avg {time1 - start}")
    
    route_dir_segments = segment_averaging_with_geometry(
        df, 
        group_cols =  OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS,
        analysis_date_list = [analysis_date],
        merge_cols = OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS             
    ).sort_values
    
    utils.geoparquet_gcs_export(
        route_dir_segments,
        SEGMENT_GCS,
        f"{ROUTE_SEG_FILE}_{analysis_date}"
    )
        
    time2 = datetime.datetime.now()
    logger.info(f"route dir seg avg {time2 - time1}")
    logger.info(f"single day segment execution time: {time2 - start}")
    
    return    


def multi_day_segment_averages(analysis_date_list: list, dict_inputs: dict):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """    
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    
    ROUTE_SEG_FILE = dict_inputs["route_dir_multi_segment"]
        
    start = datetime.datetime.now()
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        SPEED_FILE,
        analysis_date_list,
        data_type  = "df",
        get_pandas = False,
        columns = (OPERATOR_COLS + SHAPE_STOP_COLS + 
                   STOP_PAIR_COLS + ROUTE_DIR_COLS + [
                   "trip_instance_key", "speed_mph", 
                   "meters_elapsed", "sec_elapsed", 
                   "time_of_day"]),
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    )
    print("concatenated files") 

    time_span_str, time_span_num = time_helpers.time_span_labeling(
        analysis_date_list)

    route_dir_segments = delayed(segment_averaging_with_geometry)(
        df, 
        OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS + ["weekday_weekend"],
        analysis_date_list = analysis_date_list,
        merge_cols = OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS             
    )
    
    route_dir_segments = compute(route_dir_segments)[0]
    
    utils.geoparquet_gcs_export(
        route_dir_segments,
        SEGMENT_GCS,
        f"{ROUTE_SEG_FILE}_{time_span_str}"
    )
        
    end = datetime.datetime.now()
    logger.info(f"multi day segment execution time: {end - start}")
    
    return    
    
def stage_open_data_exports(analysis_date: str, dict_inputs: dict):
    """
    For the datasets we publish to Geoportal, 
    export them to a stable GCS URL so we can always 
    read it in open_data/catalog.yml.
    """
    datasets = [
        dict_inputs["route_dir_single_segment"],
        dict_inputs["route_dir_single_summary"]
    ]

    for d in datasets:
        gdf = gpd.read_parquet(
            f"{SEGMENT_GCS}{d}_{analysis_date}.parquet"
        )
        
        utils.geoparquet_gcs_export(
            gdf,
            f"{SEGMENT_GCS}export/",
            f"{Path(d).stem}"
        )
        del gdf
    
    print(f"overwrite {datasets}")
    
    return
        

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    from shared_utils import rt_dates
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()
        
        single_day_segment_averages(analysis_date, STOP_SEG_DICT)
        stage_open_data_exports(analysis_date, STOP_SEG_DICT)
        
        end = datetime.datetime.now()
        
        logger.info(f"average rollups for {analysis_date}: {end - start}")
    
        
    '''
    for one_week in [rt_dates.oct_week, rt_dates.apr_week]:
        start = datetime.datetime.now()
            
        multi_day_segment_averages(one_week, STOP_SEG_DICT)
        end = datetime.datetime.now()
    
        logger.info(f"average rollups for {one_week}: {end - start}")
    '''