"""
Multi-day segment speed averages.
Start with year-weekday/weekend-time-of-day aggregation.

TODO: test this on all the dates
"""
import dask.dataframe as dd
import datetime
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from typing import Literal, Optional

from calitp_data_analysis import utils

from segment_speed_utils import gtfs_schedule_wrangling, segment_calcs, time_series_utils
from shared_utils import publish_utils, time_helpers
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS
from segment_speed_utils.project_vars import SEGMENT_TYPES


def import_singleday_segment_speeds(
    gcs_path: str,
    input_file: str,
    analysis_date_list: list,
):
    """
    Concatenate all the dates for segment-time_of_day aggregations.
    Import as dask dataframe because we will aggregate,
    then merge back segment geometry.
    """
    # Drop geometry bc we'll merge in segment geom again
    df = time_series_utils.concatenate_datasets_across_dates(
        gcs_path,
        input_file,
        analysis_date_list,
        data_type = "df",
        get_pandas=False,
    ).drop(columns = "geometry")
    
    return df


def annual_time_of_day_averages(
    analysis_date_list: list,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional = GTFS_DATA_DICT
):
    """
    Do a year-weekday_weekend-time_of_day aggregation
    for segments.
    
    TODO: Future extensions of this would set the export file,
    import_singleday_segment_speeds, add the grouping columns,
    calculate weighted average across a different grouping,
    merge in the segment geometry,
    and add any columns from the crosswalk we want to display.
    """
    start = datetime.datetime.now()
    
    dict_inputs = config_path[segment_type]
        
    SPEED_FILE = dict_inputs["segment_timeofday"]
    EXPORT_FILE = dict_inputs["segment_timeofday_weekday_year"]
    
    SEGMENT_COLS = [*dict_inputs["segment_cols"]]
    SEGMENT_COLS_NO_GEOM = [i for i in SEGMENT_COLS if i != "geometry"]
                                      
    OPERATOR_COLS = ["schedule_gtfs_dataset_key"]
    CROSSWALK_COLS = [*dict_inputs.crosswalk_cols]
    
    df = import_singleday_segment_speeds(
        SEGMENT_GCS, 
        SPEED_FILE, 
        analysis_date_list
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    ).pipe(
        time_helpers.add_quarter
    )
       
    avg_speeds = segment_calcs.calculate_weighted_averages(
        df,
        OPERATOR_COLS + SEGMENT_COLS_NO_GEOM + ["time_of_day", "weekday_weekend", "year"],
        metric_cols = ["p20_mph", "p50_mph", "p80_mph"], 
        weight_col = "n_trips"
    ).persist()
    
    publish_utils.if_exists_then_delete(
        f"{SEGMENT_GCS}{EXPORT_FILE}"
    )
    
    avg_speeds.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}",
        partition_on = "time_of_day"
    )
    '''
    speeds_gdf = delayed(segment_calcs.merge_in_segment_geometry)(
        avg_speeds,
        analysis_date_list,
        segment_type,
        SEGMENT_COLS
    ).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        analysis_date_list,
        columns = CROSSWALK_COLS
    )
    
    utils.geoparquet_gcs_export(
        speeds_gdf,
        SEGMENT_GCS,
        EXPORT_FILE
    )
    '''

    end = datetime.datetime.now()
    
    logger.info(
        f"{segment_type}: weekday/time-of-day averages for {analysis_date_list} "
        f"execution time: {end - start}"
    )
    
    return 

if __name__ == "__main__":
    
    from shared_utils import rt_dates 
    
    LOG_FILE = "../logs/avg_speeds.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    annual_time_of_day_averages(
        rt_dates.all_dates,
        segment_type = "rt_stop_times",
    )
    