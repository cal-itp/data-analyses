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

from segment_speed_utils import gtfs_schedule_wrangling, helpers, segment_calcs
from shared_utils import time_helpers
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS
from segment_speed_utils.project_vars import SEGMENT_TYPES

def merge_schedule_columns_for_speedmaps(
    df: pd.DataFrame,
    analysis_date: str
):
    """
    Merge in additional columns from scheduled trip table.
    Want route_short_name, as well as n_scheduled_trips and 
    trips_per_hour by time-of-day.
    """
    keep_trip_cols = [
        'trip_instance_key', 'gtfs_dataset_key', 
        'route_id',
        'shape_id', 'route_short_name'
    ]

    time_buckets = gtfs_schedule_wrangling.get_trip_time_buckets(analysis_date)
   
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = keep_trip_cols,
        get_pandas = True
    ).merge(
        time_buckets,
        on = "trip_instance_key",
        how = "inner"
    )
    
    route_cols = [
        "schedule_gtfs_dataset_key", "route_id", "shape_id"
    ]    
    
    # Get trip counts by time-of-day
    schedule_trip_counts = gtfs_schedule_wrangling.count_trips_by_group(
        trips,
        group_cols = route_cols + ["time_of_day"]
    ).rename(columns = {'n_trips': 'n_trips_sch'})
    
    # Add a trips per hour within each time-of-day bucket
    schedule_trip_counts['trips_hr_sch'] = schedule_trip_counts.apply(
        lambda x: round(x.n_trips_sch / time_helpers.HOURS_BY_TIME_OF_DAY[x.time_of_day], 3), 
        axis=1)
    
    df2 = pd.merge(
        df,
        schedule_trip_counts,
        on = route_cols + ["time_of_day"],
        how = "inner"
    ).merge(
        trips[route_cols + ["route_short_name"]].drop_duplicates(),
        on = route_cols,
        how = "inner"
    )
    
    return df2


def aggregate_by_time_of_day(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional = GTFS_DATA_DICT
):
    """
    Set the time-of-day single day aggregation
    and calculate 20th/50th/80th percentile speeds.
    These daily metrics feed into multi-day metrics.
    """
    start = datetime.datetime.now()
    
    dict_inputs = config_path[segment_type]
        
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    EXPORT_FILE = dict_inputs["segment_timeofday"]
    
    SEGMENT_COLS = [*dict_inputs["segment_cols"]]
    SEGMENT_COLS = [i for i in SEGMENT_COLS if i != "geometry"]
                                      
    OPERATOR_COLS = ["schedule_gtfs_dataset_key"]
    CROSSWALK_COLS = [*dict_inputs["crosswalk_cols"]]
    
    group_cols = OPERATOR_COLS + SEGMENT_COLS + ["stop_pair_name", "time_of_day"]
    
    df = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}.parquet",
        columns = OPERATOR_COLS + SEGMENT_COLS + [
            "trip_instance_key", "stop_pair_name", 
            "time_of_day", "speed_mph"],
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    ).dropna(subset="speed_mph").pipe(
        segment_calcs.calculate_avg_speeds,
        group_cols
    )
    
    if segment_type == "speedmap_segments":
        df = delayed(merge_schedule_columns_for_speedmaps)(
            df, analysis_date
        ).pipe(
            gtfs_schedule_wrangling.merge_operator_identifiers, 
            [analysis_date],
            columns = CROSSWALK_COLS
        )
    
    avg_speeds_with_geom = delayed(segment_calcs.merge_in_segment_geometry)(
        df,
        [analysis_date],
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
    
    return avg_speeds_with_geom



if __name__ == "__main__":
    '''
    from segment_speed_utils.project_vars import analysis_date_list     
    
    LOG_FILE = "../logs/speeds_timeofday.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    delayed_dfs = [
        delayed(aggregate_by_time_of_day)(
            analysis_date = analysis_date, 
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        ) for analysis_date in analysis_date_list
    ]
    
    [compute(i)[0] for i in delayed_dfs]
    '''