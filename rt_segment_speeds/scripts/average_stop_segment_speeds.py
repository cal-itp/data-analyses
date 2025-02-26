"""
Cache a new time-of-day single day grain.
Use that to build all the other aggregations
for multiday. 
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from calitp_data_analysis import utils
from dask import delayed, compute
from loguru import logger
from typing import Literal, Optional

from segment_speed_utils import gtfs_schedule_wrangling, segment_calcs
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT


def aggregate_to_peak_offpeak(
    analysis_date: str,
    segment_type: str = "stop_segments", 
    config_path: Optional = GTFS_DATA_DICT
):
    """
    Set the peak/offpeak/all_day single day aggregation
    using weighted averages to calculate 20th/50th/80th percentile speeds.
    """
    start = datetime.datetime.now()
    
    dict_inputs = config_path[segment_type]
        
    SPEED_FILE = dict_inputs["segment_timeofday"]
    EXPORT_FILE = dict_inputs["segment_peakoffpeak"]
    
    SEGMENT_COLS = [*dict_inputs["segment_cols"]]
    SEGMENT_COLS_NO_GEOM = [i for i in SEGMENT_COLS if i != "geometry"]
                                      
    OPERATOR_COLS = ["schedule_gtfs_dataset_key"]
    CROSSWALK_COLS = [*dict_inputs.crosswalk_cols]
    
    df = gpd.read_parquet(
        f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}.parquet",
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    )
    
    segment_gdf = df[OPERATOR_COLS + SEGMENT_COLS].drop_duplicates()
    
    peak_offpeak = segment_calcs.calculate_weighted_averages(
        df,
        OPERATOR_COLS + SEGMENT_COLS_NO_GEOM + ["stop_pair_name", "peak_offpeak"],
        metric_cols = ["p20_mph", "p50_mph", "p80_mph"], 
        weight_col = "n_trips"
    ).rename(columns = {"peak_offpeak": "time_period"})
    
    all_day = segment_calcs.calculate_weighted_averages(
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
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    segment_type = "stop_segments"

    for analysis_date in analysis_date_list:
        aggregate_to_peak_offpeak(analysis_date, segment_type)
    
