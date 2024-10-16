"""
Quick aggregation for segment speed averages.
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
                                 helpers, 
                                 metrics,
                                 time_helpers, 
                                 time_series_utils
                                 )
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS
from segment_speed_utils.time_series_utils import ROUTE_DIR_COLS
from segment_speed_utils.project_vars import SEGMENT_TYPES

OPERATOR_COLS = [
    "schedule_gtfs_dataset_key", 
]

CROSSWALK_COLS = [
    "schedule_gtfs_dataset_key", "name",
    "caltrans_district",
    "organization_source_record_id", "organization_name",
    "base64_url"
]

def import_segments(
    analysis_date_list: list,
    segment_type: Literal[SEGMENT_TYPES],
    get_pandas: bool
) -> gpd.GeoDataFrame:
    """
    Import the segments to merge.
    For stop_segments, import only 1 trip per shape for segments, and merge
    on shape_array_key and stop_pair.
    For rt_stop_times, import all trips with their segments, and merge on 
    trip_instance_key and stop_pair.
    """
    SEGMENT_FILE = GTFS_DATA_DICT[segment_type].segments_file

    dfs = [
        delayed(gpd.read_parquet)(
            f"{SEGMENT_GCS}{SEGMENT_FILE}_{analysis_date}.parquet",
        ).to_crs(WGS84) for analysis_date in analysis_date_list
    ]
    
    gdf = delayed(pd.concat)(
        dfs, axis=0, ignore_index=True
    )
    
    if get_pandas:
        gdf = compute(gdf)[0]
    
    return gdf


def segment_averaging_with_geometry(
    gdf: gpd.GeoDataFrame,
    group_cols: list, 
    analysis_date_list: list,
    segment_type: Literal[SEGMENT_TYPES]
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
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        analysis_date_list,
        columns = CROSSWALK_COLS
    )
    
    col_order = [c for c in avg_speeds.columns]
    
    segment_geom = import_segments(
        [analysis_date], segment_type, get_pandas=True
    )
    
    # The merge columns list should be all the columns that are in common
    # between averaged speeds and segment gdf
    segment_file_cols = segment_geom.columns.tolist()
    merge_cols = list(set(col_order).intersection(segment_file_cols))
    
    avg_speeds_with_geom = pd.merge(
        segment_geom[merge_cols + ["geometry"]].drop_duplicates(),
        avg_speeds,
        on = merge_cols, 
    ).reset_index(drop=True).reindex(
        columns = col_order + ["geometry"]
    )
    
    return avg_speeds_with_geom


def single_day_segment_averages(
    analysis_date: str, 
    segment_type: Literal[SEGMENT_TYPES]
):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """    
    dict_inputs = GTFS_DATA_DICT[segment_type]
    
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
 
    # These are the grouping columns (list) to use for the shape and route-dir aggregation
    SHAPE_STOP_COLS = [*dict_inputs["shape_stop_cols"]]
    STOP_PAIR_COLS = [*dict_inputs["stop_pair_cols"]]

    SHAPE_SEG_FILE = dict_inputs["shape_stop_single_segment"]
    ROUTE_SEG_FILE = dict_inputs["route_dir_single_segment"]
        
    start = datetime.datetime.now()
    columns = (OPERATOR_COLS + SHAPE_STOP_COLS + 
                   STOP_PAIR_COLS + ROUTE_DIR_COLS + [
                       "trip_instance_key", "speed_mph", 
                       "meters_elapsed", "sec_elapsed", 
                       "time_of_day"])
    #  https://stackoverflow.com/questions/7961363/removing-duplicates-in-lists
    #  route_id will be in both shape stop and route dir; this method maintains list order
    columns = list(dict.fromkeys(columns))
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        SPEED_FILE,
        [analysis_date],
        data_type  = "df",
        get_pandas = True,
        columns = columns,
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
        segment_type = segment_type
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
        group_cols = OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS,
        analysis_date_list = [analysis_date],
        segment_type = segment_type
    )
    
    utils.geoparquet_gcs_export(
        route_dir_segments,
        SEGMENT_GCS,
        f"{ROUTE_SEG_FILE}_{analysis_date}"
    )
        
    time2 = datetime.datetime.now()
    logger.info(f"route dir seg avg {time2 - time1}")
    logger.info(f"single day segment {analysis_date} execution time: {time2 - start}")
    
    return    


def multi_day_segment_averages(
    analysis_date_list: list, 
    segment_type: Literal[SEGMENT_TYPES]
):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """   
    dict_inputs = GTFS_DATA_DICT[segment_type]

    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    
    # These are the grouping columns (list) to use for the shape and route-dir aggregation
    SHAPE_STOP_COLS = [*dict_inputs["shape_stop_cols"]]
    STOP_PAIR_COLS = [*dict_inputs["stop_pair_cols"]]    
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
        segment_type = segment_type
    )
    
    route_dir_segments = compute(route_dir_segments)[0]
    
    utils.geoparquet_gcs_export(
        route_dir_segments,
        SEGMENT_GCS,
        f"{ROUTE_SEG_FILE}_{time_span_str}"
    )
        
    end = datetime.datetime.now()
    logger.info(f"multi day segment {analysis_date_list} execution time: {end - start}")
    
    return    
        

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    from shared_utils import rt_dates
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    segment_type = "stop_segments"
    
    for analysis_date in analysis_date_list:

        single_day_segment_averages(analysis_date, segment_type)
    
    '''
    from segment_speed_utils.project_vars import weeks_available
    
    for one_week in weeks_available:
        
        multi_day_segment_averages(one_week, segment_type)
    '''