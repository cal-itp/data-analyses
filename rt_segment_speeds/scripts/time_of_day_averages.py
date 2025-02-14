"""
Cache a new time-of-day single day grain.
Use that to build all the other aggregations
for multiday. 

TODO: make clear year/quarter or weekday/weekend grain
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from calitp_data_analysis import utils
from dask import delayed, compute
from loguru import logger
from typing import Literal

from segment_speed_utils import gtfs_schedule_wrangling, segment_calcs
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              GTFS_DATA_DICT, 
                                              SEGMENT_TYPES)
import average_segment_speeds

CROSSWALK_COLS = average_segment_speeds.CROSSWALK_COLS

def aggregate_by_time_of_day(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES]
):
    """
    Set the time-of-day single day aggregation
    and calculate 20th/50th/80th percentile speeds.
    These daily metrics feed into multi-day metrics.
    """
    start = datetime.datetime.now()
    
    dict_inputs = GTFS_DATA_DICT[segment_type]
        
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    EXPORT_FILE = dict_inputs["segment_timeofday"]
    
    SEGMENT_COLS = [*dict_inputs["segment_cols"]]
    SEGMENT_COLS = [i for i in SEGMENT_COLS if i != "geometry"]
                                      
    OPERATOR_COLS = ["schedule_gtfs_dataset_key"]
     
    df = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}.parquet",
        columns = OPERATOR_COLS + SEGMENT_COLS + [
            "trip_instance_key", "stop_pair_name", 
            "time_of_day", "speed_mph"],
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    ).dropna(subset="speed_mph").pipe(
        segment_calcs.calculate_avg_speeds,
        OPERATOR_COLS + SEGMENT_COLS + ["stop_pair_name", "time_of_day"]
    )
    
    avg_speeds_with_geom = delayed(average_segment_speeds.merge_in_segment_geometry)(
        df,
        analysis_date,
        segment_type,
    )
    
    avg_speeds_with_geom = compute(avg_speeds_with_geom)[0]

    utils.geoparquet_gcs_export(
        avg_speeds_with_geom,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
                    
    end = datetime.datetime.now()
    logger.info(
        f"{segment_type}: time-of-day averages for {analysis_date} "
        f"execution time: {end - start}"
    )
    
    return


def calculate_weighted_averages(
    df: pd.DataFrame, 
    group_cols: list, 
    metric_cols: list, 
    weight_col: str
):
    """
    can we use dask_utils to put together
    several days, weight the speed by n_trips,
    and roll it up further?
    """    
    for c in metric_cols:
        df[c] = df[c] * df[weight_col]    
    
    df2 = (df.groupby(group_cols, group_keys=False)
           .agg({c: "sum" for c in metric_cols + [weight_col]})
           .reset_index()
          )
    
    for c in metric_cols:
        df2[c] = df2[c].divide(df2[weight_col]).round(2)
    
    return df2


def aggregate_to_peak_offpeak(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES]
):
    """
    Set the peak/offpeak/all_day single day aggregation
    using weighted averages to calculate 20th/50th/80th percentile speeds.
    """
    start = datetime.datetime.now()
    
    dict_inputs = GTFS_DATA_DICT[segment_type]
        
    SPEED_FILE = dict_inputs["segment_timeofday"]
    EXPORT_FILE = dict_inputs["route_dir_single_segment"]
    
    SEGMENT_COLS = [*dict_inputs["segment_cols"]]
    SEGMENT_COLS_NO_GEOM = [i for i in SEGMENT_COLS if i != "geometry"]
                                      
    OPERATOR_COLS = ["schedule_gtfs_dataset_key"]
     
    df = gpd.read_parquet(
        f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}.parquet",
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    )
    
    segment_gdf = df[SEGMENT_COLS].drop_duplicates()
    
    peak_offpeak = calculate_weighted_averages(
        df,
        OPERATOR_COLS + SEGMENT_COLS_NO_GEOM + ["stop_pair_name", "peak_offpeak"],
        metric_cols = ["p20_mph", "p50_mph", "p80_mph"], 
        weight_col = "n_trips"
    ).rename(columns = {"peak_offpeak": "time_period"})
    
    all_day = calculate_weighted_averages(
        df,
        OPERATOR_COLS + SEGMENT_COLS_NO_GEOM + ["stop_pair_name"],
        metric_cols = ["p20_mph", "p50_mph", "p80_mph"], 
        weight_col = "n_trips"
    ).assign(
        time_period= "all_day"
    )
    
    df2 = pd.concat(
        [peak_offpeak, all_day], 
        axis=0, ignore_index=True
    ).sort_values(
        OPERATOR_COLS + SEGMENT_COLS_NO_GEOM + ["time_period"]
    ).reset_index(drop=True).pipe(
        gtfs_schedule_wrangling.merge_operator_identifiers, 
        [analysis_date],
        columns = CROSSWALK_COLS
    ) 
    
    avg_speeds_with_geom = pd.merge(
        segment_gdf,
        df2,
        on = SEGMENT_COLS_NO_GEOM,
        how = "inner"
    )
    
    utils.geoparquet_gcs_export(
        avg_speeds_with_geom,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
        
                    
    end = datetime.datetime.now()
    logger.info(
        f"{segment_type}: peak/offpeak for {analysis_date} "
        f"execution time: {end - start}"
    )


if __name__ == "__main__":

    from shared_utils import rt_dates
    analysis_date_list = [rt_dates.DATES[f"{m}2024"] for m in ["oct", "nov", "dec"]]
    
    LOG_FILE = "../logs/test.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    segment_type = "stop_segments"

    for analysis_date in analysis_date_list:
        aggregate_by_time_of_day(analysis_date, segment_type)
        aggregate_to_peak_offpeak(analysis_date, segment_type)