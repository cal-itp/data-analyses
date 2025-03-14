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
from calitp_data_analysis.geography_utils import WGS84

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


def export_segment_geometry(
    year: str,
):
    """
    Dedupe segment geometries using columns, 
    since geometries may slightly differ.
    Visual inspection shows start and endpoints might be
    slightly different but still capture the same corridor.
    
    Big Blue Bus: stop_pair = "1115__187"
    In 2024, there are 4 rows, but the 4 rows are basically the same,
    so let's keep the most recent row.
    """
    SEGMENTS_FILE = GTFS_DATA_DICT.rt_stop_times.segments_file
    EXPORT_FILE = GTFS_DATA_DICT.rt_stop_times.segments_year_file
    
    keep_cols = [
        "schedule_gtfs_dataset_key", 
        "route_id", "direction_id", 
        "stop_pair", 
    ]
    
    dates_in_year = [
        date for date in rt_dates.all_dates if year in date
    ]
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        SEGMENTS_FILE,
        dates_in_year,
        columns = keep_cols + ["geometry"],
        data_type = "gdf",
        get_pandas= False,        
    ).sort_values(
        "service_date", ascending=False
    ).drop(
        columns = "service_date"
    ).drop_duplicates(
        subset = keep_cols
    ).reset_index(drop=True).to_crs(WGS84)

    df = df.compute()

    df.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{year}.parquet",
    )
    
    print(f"exported stop segments for year {year}")
        
    return 
    

def annual_time_of_day_averages(
    year: str,
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
    SEGMENTS_YEAR_FILE = dict_inputs["segments_year_file"]
    EXPORT_FILE = dict_inputs["segment_timeofday_weekday_year"]
    
    SEGMENT_COLS = [*dict_inputs["segment_cols"]]
    SEGMENT_COLS_NO_GEOM = [i for i in SEGMENT_COLS if i != "geometry"]
                                      
    OPERATOR_COLS = ["schedule_gtfs_dataset_key"]
    CROSSWALK_COLS = [*dict_inputs.crosswalk_cols]
    
    analysis_date_list = [
        date for date in rt_dates.all_dates if year in date
    ]
    
    df = import_singleday_segment_speeds(
        SEGMENT_GCS, 
        SPEED_FILE, 
        analysis_date_list
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    ).pipe(
        time_helpers.add_quarter
    )
    
    group_cols = OPERATOR_COLS + SEGMENT_COLS_NO_GEOM + [
        "time_of_day", "weekday_weekend", "year"]
    
    speed_cols = ["p20_mph", "p50_mph", "p80_mph"]
    weight_col = "n_trips"
    
    orig_dtypes = df[group_cols + speed_cols + [weight_col]].dtypes.to_dict()
    
    avg_speeds = df.map_partitions(
        segment_calcs.calculate_weighted_averages,
        OPERATOR_COLS + SEGMENT_COLS_NO_GEOM + ["time_of_day", "weekday_weekend", "year"],
        metric_cols = speed_cols, 
        weight_col = weight_col,
        meta = {
            **orig_dtypes,
        },
        align_dataframes = False
    ).compute().pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers,
        analysis_date_list,
        columns = CROSSWALK_COLS
    )
    
    avg_speeds.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{year}.parquet"
    )

    end = datetime.datetime.now()
    
    logger.info(
        f"{segment_type}: weekday/time-of-day averages for {year} "
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
    
    # isolate segments per year to allow for export
    # rerun previous years when necessary
    for year in ["2025"]:
        
        export_segment_geometry(year)
    
        annual_time_of_day_averages(
            year,
            segment_type = "rt_stop_times",
        )
    

        

    