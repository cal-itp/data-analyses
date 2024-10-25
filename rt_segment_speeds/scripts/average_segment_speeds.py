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

def concatenate_trip_segment_speeds(
    analysis_date_list: list,
    segment_type: Literal[SEGMENT_TYPES],
    **kwargs
) -> pd.DataFrame:
    """
    Concatenate segment speeds at trip grain for
    as many dates to average over.
    
    Add the columns we need for peak/offpeak 
    and weekday/weekend that will support
    peak/offpeak segment weighted averages.
    """
    dict_inputs = GTFS_DATA_DICT[segment_type]
    
    SHAPE_STOP_COLS = [*dict_inputs["shape_stop_cols"]]
    ROUTE_DIR_COLS = [*dict_inputs["route_dir_cols"]]
    STOP_PAIR_COLS = [*dict_inputs["stop_pair_cols"]]
    
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS, 
        SPEED_FILE,
        analysis_date_list,
        data_type  = "df",
        columns = helpers.unique_list(
            OPERATOR_COLS + SHAPE_STOP_COLS + 
            ROUTE_DIR_COLS + STOP_PAIR_COLS + [
                "trip_instance_key", "speed_mph", 
                "meters_elapsed", "sec_elapsed", 
                "time_of_day"]),
        filters = [[("speed_mph", "<=", MAX_SPEED)]],
        **kwargs
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    )
    print("concatenated files") 
    
    return df


def merge_in_segment_geometry(
    speeds_by_segment: pd.DataFrame,
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES],
) -> gpd.GeoDataFrame:
    """
    Import the segments to merge and attach it to the average speeds.
    For a week's worth of data, we'll just use Wed segments.
    """
    SEGMENT_FILE = GTFS_DATA_DICT[segment_type].segments_file

    segment_geom = gpd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}_{analysis_date}.parquet",
    ).to_crs(WGS84)
    
    col_order = [c for c in speeds_by_segment.columns]
    
    # The merge columns list should be all the columns that are in common
    # between averaged speeds and segment gdf
    geom_file_cols = segment_geom.columns.tolist()
    merge_cols = list(set(col_order).intersection(geom_file_cols))
    
    gdf = pd.merge(
        segment_geom[merge_cols + ["geometry"]].drop_duplicates(),
        speeds_by_segment,
        on = merge_cols, 
    ).reset_index(drop=True).reindex(
        columns = col_order + ["geometry"]
    )
    
    return gdf


def segment_averages(
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
    start = datetime.datetime.now()
    
    df = concatenate_trip_segment_speeds(
        analysis_date_list,
        segment_type,
        get_pandas = False
    )
    
    avg_speeds = delayed(metrics.concatenate_peak_offpeak_allday_averages)(
        df, 
        group_cols,
        metric_type = "segment_speeds"
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        analysis_date_list,
        columns = CROSSWALK_COLS
    ).persist()

    if len(analysis_date_list) > 1:
        # If a week (date list) is put in, use Wednesday for segment geometry
        time_span_str, _ = time_helpers.time_span_labeling(
            analysis_date_list)
        
        analysis_date = analysis_date_list[2]
    
    else:
        # If a single day is put in, use that date for segment geometry
        analysis_date = analysis_date_list[0]
        time_span_str = analysis_date
      
    avg_speeds_with_geom = delayed(merge_in_segment_geometry)(
        avg_speeds,
        analysis_date, 
        segment_type
    )
        
    avg_speeds_with_geom = compute(avg_speeds_with_geom)[0]
    
    utils.geoparquet_gcs_export(
        avg_speeds_with_geom,
        SEGMENT_GCS,
        f"{export_file}_{time_span_str}"
    )
        
    end = datetime.datetime.now()
    logger.info(f"segment speed averaging for {analysis_date_list} execution time: {end - start}")
    
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
    
    dict_inputs = GTFS_DATA_DICT[segment_type]
    ROUTE_DIR_COLS = [*dict_inputs["route_dir_cols"]]
    STOP_PAIR_COLS = [*dict_inputs["stop_pair_cols"]]
    
    ROUTE_SEG_FILE = dict_inputs["route_dir_single_segment"]

    for analysis_date in analysis_date_list:

        segment_averages(
            [analysis_date], 
            segment_type, 
            group_cols = OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS,
            export_file = ROUTE_SEG_FILE
        )
    
    '''
    from segment_speed_utils.project_vars import weeks_available
    
    ROUTE_SEG_FILE = dict_inputs["route_dir_multi_segment"]

    for one_week in weeks_available:
        
        segment_averages(
            one_week, 
            segment_type,
            group_cols = OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS + ["weekday_weekend"],
            export_file = ROUTE_SEG_FILE
        )
    '''       