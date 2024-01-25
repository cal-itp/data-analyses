"""
Quick aggregation for speed metrics.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from segment_speed_utils import (gtfs_schedule_wrangling, helpers, 
                                 segment_calcs, time_helpers)
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH


OPERATOR_COLS = [
    "schedule_gtfs_dataset_key", 
    #"name", # from schedule
    #"organization_source_record_id", "organization_name", # from dim_organizations
    #"base64_url", "caltrans_district", 
]

SHAPE_STOP_COLS = [
    "shape_array_key", "shape_id", "stop_sequence",
]

STOP_PAIR_COLS = ["stop_pair"] 

ROUTE_DIR_COLS = [
    "route_id", "direction_id"
]

def merge_operator_identifiers(
    df: pd.DataFrame, 
    analysis_date_list: list
) -> pd.DataFrame:
    """
    Carrying a lot of these operator identifiers is not 
    inconsequential, esp when we need to run a week's segment speeds
    in one go.
    Instead, we'll just merge it back on before we export.
    """
    crosswalk = pd.concat([
        helpers.import_schedule_gtfs_key_organization_crosswalk(
            analysis_date,
        ).drop(columns = "itp_id") 
        for analysis_date in analysis_date_list],
        axis=0, ignore_index=True
    ).drop_duplicates()
    
    df = pd.merge(
        df,
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )
    
    return df
    
    
def weighted_average_speeds_across_segments(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame: 
    """
    We can use our segments and the deltas within a trip
    to calculate the trip-level average speed, or
    the route-direction-level average speed.
    But, we want a weighted average, using the raw deltas
    instead of mean(speed_mph), since segments can be varying lengths.
    """
    avg_speeds_peak = (df.groupby(group_cols + ["peak_offpeak"], 
                      observed=True, group_keys=False)
           .agg({
               "meters_elapsed": "sum",
               "sec_elapsed": "sum",
           }).reset_index()
          )
    
    avg_speeds_peak = segment_calcs.speed_from_meters_elapsed_sec_elapsed(
        avg_speeds_peak)
    
    avg_speeds_allday = (df.groupby(group_cols, 
                                    observed=True, group_keys=False)
                         .agg({
                             "meters_elapsed": "sum",
                             "sec_elapsed": "sum",
                         }).reset_index()
                        )
    
    avg_speeds_allday = segment_calcs.speed_from_meters_elapsed_sec_elapsed(
        avg_speeds_allday
    ).assign(
        peak_offpeak = "all_day"
    )
    
    avg_speeds = pd.concat(
        [avg_speeds_peak, avg_speeds_allday],
        axis=0, ignore_index = True
    ).rename(
        columns = {"peak_offpeak": "time_period"}
    )
    
    return avg_speeds
    

def concatenate_peak_offpeak_allday_averages(
    df: pd.DataFrame, 
    group_cols: list
) -> pd.DataFrame:
    """
    Calculate average speeds for all day and
    peak_offpeak.
    Concatenate these, so that speeds are always calculated
    for the same 3 time periods.
    """
    avg_speeds_peak = segment_calcs.calculate_avg_speeds(
        df,
        group_cols + ["peak_offpeak"]
    )
    
    avg_speeds_allday = segment_calcs.calculate_avg_speeds(
        df,
        group_cols
    ).assign(peak_offpeak = "all_day")
    
    # Concatenate so that every segment has 3 time periods: peak, offpeak, and all_day
    avg_speeds = pd.concat(
        [avg_speeds_peak, avg_speeds_allday], 
        axis=0, ignore_index = True
    ).rename(
        columns = {"peak_offpeak": "time_period"}
    )
        
    return avg_speeds


def concatenate_trip_segment_speeds(
    analysis_date_list: list,
    dict_inputs: dict
) -> pd.DataFrame:
    """
    Concatenate the speed-trip parquets together, 
    whether it's for single day or multi-day averages.
    Add columns for peak_offpeak, weekday_weekend based 
    on day of week and time-of-day.
    """
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    
    df = pd.concat([
        pd.read_parquet(
            f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}.parquet", 
            columns = (OPERATOR_COLS + SHAPE_STOP_COLS + 
                       STOP_PAIR_COLS + ROUTE_DIR_COLS + [
                           "trip_instance_key", "speed_mph", 
                           "meters_elapsed", "sec_elapsed", 
                           "time_of_day"]),
            filters = [[("speed_mph", "<=", MAX_SPEED)]]
        ).assign(
            service_date = pd.to_datetime(analysis_date)
        ) for analysis_date in analysis_date_list], 
        axis=0, ignore_index = True
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    )
    
    return df
    
    
def single_day_averages(analysis_date: str, dict_inputs: dict):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """
    SHAPE_SEG_FILE = dict_inputs["shape_stop_single_segment"]
    ROUTE_SEG_FILE = dict_inputs["route_dir_single_segment"]
    TRIP_FILE = dict_inputs["trip_speeds_single_summary"]
    ROUTE_DIR_FILE = dict_inputs["route_dir_single_summary"]
    
    start = datetime.datetime.now()
    
    df = concatenate_trip_segment_speeds([analysis_date], dict_inputs)
    print("concatenated files")   
    
    t0 = datetime.datetime.now()
    shape_stop_segments = concatenate_peak_offpeak_allday_averages(
        df, 
        OPERATOR_COLS + SHAPE_STOP_COLS + STOP_PAIR_COLS
    ).pipe(
        merge_operator_identifiers, [analysis_date]
    )
    
    shape_stop_segments.to_parquet(
        f"{SEGMENT_GCS}{SHAPE_SEG_FILE}_{analysis_date}.parquet"
    )
    del shape_stop_segments
    
    t1 = datetime.datetime.now()
    logger.info(f"shape seg avg {t1 - t0}")
    
    route_dir_segments = concatenate_peak_offpeak_allday_averages(
        df, 
        OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS
    ).pipe(
        merge_operator_identifiers, [analysis_date]
    )
    
    route_dir_segments.to_parquet(
        f"{SEGMENT_GCS}{ROUTE_SEG_FILE}_{analysis_date}.parquet"
    )
    del route_dir_segments
    
    t2 = datetime.datetime.now()
    logger.info(f"route dir seg avg {t2 - t1}")
    
    trip_avg = weighted_average_speeds_across_segments(
        df,
        OPERATOR_COLS + ROUTE_DIR_COLS + [
            "trip_instance_key", 
            "shape_array_key", "shape_id", "time_of_day"]
    ).pipe(
        merge_operator_identifiers, [analysis_date]
    )
    
    trip_avg.to_parquet(
        f"{SEGMENT_GCS}{TRIP_FILE}_{analysis_date}.parquet"
    )
    del trip_avg
    
    t3 = datetime.datetime.now()
    logger.info(f"trip avg {t3 - t2}")
    
    route_dir_avg = weighted_average_speeds_across_segments(
        df,
        OPERATOR_COLS + ROUTE_DIR_COLS
    ).pipe(
        merge_operator_identifiers, [analysis_date]
    )
    
    route_dir_avg.to_parquet(
        f"{SEGMENT_GCS}{ROUTE_DIR_FILE}_{analysis_date}.parquet"
    )
    del route_dir_avg
    
    t4 = datetime.datetime.now()
    logger.info(f"route dir avg: {t4 - t3}")
    
    return


def multi_day_averages(analysis_date_list: list, dict_inputs: dict):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    The main difference from a single day average is that
    the seven days is concatenated first before averaging,
    so that we get weighted averages.
    """
    ROUTE_SEG_FILE = dict_inputs["route_dir_multi_summary"]
    ROUTE_DIR_FILE = dict_inputs["route_dir_multi_segment"]
        
    df = delayed(concatenate_trip_segment_speeds)(analysis_date_list, dict_inputs)
    print("concatenated files")   
    
    time_span_str, time_span_num = time_helpers.time_span_labeling(analysis_date_list)
    
    t0 = datetime.datetime.now()
        
    route_dir_segments = delayed(concatenate_peak_offpeak_allday_averages)(
        df, 
        OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS + ["weekday_weekend"]
    )
    
    route_dir_segments = compute(route_dir_segments)[0]
    
    route_dir_segments = time_helpers.add_time_span_columns(
        route_dir_segments, time_span_num
    ).pipe(
        merge_operator_identifiers, analysis_date_list
    )
    
    route_dir_segments.to_parquet(
        f"{SEGMENT_GCS}{ROUTE_SEG_FILE}_{time_span_str}.parquet"
    )
    del route_dir_segments   
    
    t1 = datetime.datetime.now()
    logger.info(f"route seg avg {t1 - t0}")
    
    route_dir_avg = delayed(weighted_average_speeds_across_segments)(
        df,
        OPERATOR_COLS + ROUTE_DIR_COLS + ["weekday_weekend"]
    )
    
    route_dir_avg = compute(route_dir_avg)[0]
    route_dir_avg = time_helpers.add_time_span_columns(
        route_dir_avg, time_span_num
    ).pipe(
        merge_operator_identifiers, analysis_date_list
    )

    route_dir_avg.to_parquet(
        f"{SEGMENT_GCS}{ROUTE_DIR_FILE}_{time_span_str}.parquet"
    )
    del route_dir_avg

    t2 = datetime.datetime.now()
    logger.info(f"route dir avg {t2 - t1}")
        
    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    
    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()
        single_day_averages(analysis_date, STOP_SEG_DICT)
        end = datetime.datetime.now()
        
        logger.info(f"average rollups for {analysis_date}: {end - start}")
    
    '''
    start = datetime.datetime.now()
    multi_day_averages(analysis_date_list, STOP_SEG_DICT)
    end = datetime.datetime.now()
    
    logger.info(f"average rollups for {analysis_date_list}: {end - start}")
    '''
    