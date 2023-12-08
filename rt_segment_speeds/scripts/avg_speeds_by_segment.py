"""
Quick aggregation for speed metrics by segment
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH
from calitp_data_analysis import utils, geography_utils
from shared_utils import portfolio_utils, rt_utils
    

def calculate_avg_speeds(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Calculate the median, 20th, and 80th percentile speeds 
    by groups.
    """
    # pd.groupby and pd.quantile is so slow
    # create our own list of speeds and use np
    df2 = (df.groupby(group_cols, 
                      observed=True, group_keys=False)
           .agg({"speed_mph": lambda x: sorted(list(x))})
           .reset_index()
           .rename(columns = {"speed_mph": "speed_mph_list"})
    )
                        
    df2 = df2.assign(
        p50_mph = df2.apply(lambda x: np.percentile(x.speed_mph_list, 0.5), axis=1),
        n_trips = df2.apply(lambda x: len(x.speed_mph_list), axis=1).astype("int"),
        p20_mph = df2.apply(lambda x: np.percentile(x.speed_mph_list, 0.2), axis=1),
        p80_mph = df2.apply(lambda x: np.percentile(x.speed_mph_list, 0.8), axis=1),
    )
    
    stats = df2.drop(columns = "speed_mph_list")
    
    # Clean up for map
    speed_cols = [c for c in stats.columns if "_mph" in c]
    stats[speed_cols] = stats[speed_cols].round(2)
    
    return stats
    
    
def speeds_with_segment_geom(
    analysis_date: str, 
    dict_inputs: dict = {},
): 
    """
    Import the segment-trip table. 
    Average the speed_mph across all trips present in the segment.
    """
    start = datetime.datetime.now()
    
    SEGMENT_FILE = f'{dict_inputs["segments_file"]}_{analysis_date}'
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    SPEEDS_FILE = f'{dict_inputs["stage4"]}_{analysis_date}'
    EXPORT_FILE = f'{dict_inputs["stage5"]}_{analysis_date}'
    MAX_SPEED = dict_inputs["max_speed"]
    
    # Read in speeds and attach time-of-day
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_FILE}.parquet", 
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    )
    
    time_of_day_df = sched_rt_utils.get_trip_time_buckets(analysis_date)

    df2 = pd.merge(
        df,
        time_of_day_df,
        on = "trip_instance_key",
        how = "inner"
    )
    
    subset_shape_keys = df2.shape_array_key.unique().tolist()
    
    # Load in segment geometry, keep shapes present in speeds  
    segments = gpd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}.parquet",
        columns = SEGMENT_IDENTIFIER_COLS + [
            "schedule_gtfs_dataset_key", 
            "stop_id",
            "loop_or_inlining",
            "geometry", 
            "district_name"        
        ],
        filters = [[("shape_array_key", "in", subset_shape_keys)]]
    ).to_crs(geography_utils.WGS84)
    
    all_day = calculate_avg_speeds(
        df2,
        SEGMENT_IDENTIFIER_COLS,
    )
                              
    peak = calculate_avg_speeds(
        df2[df2.time_of_day.isin(["AM Peak", "PM Peak"])],
        SEGMENT_IDENTIFIER_COLS,
    )

    stats = pd.concat([
        all_day.assign(time_of_day = "all_day"),
        peak.assign(time_of_day = "peak")
    ], axis=0)
    
    
    # Merge in segment geometry    
    gdf = pd.merge(
        segments,
        stats,
        on = SEGMENT_IDENTIFIER_COLS,
        how = "left"
    ).sort_values(
        SEGMENT_IDENTIFIER_COLS + ["time_of_day"]
    ).reset_index(drop=True)
    
    utils.geoparquet_gcs_export(
        gdf,
        SEGMENT_GCS,
        EXPORT_FILE
    )
    
    end = datetime.datetime.now()
    logger.info(f"segment averages execution time: {end - start}")

    return


def add_scheduled_trip_columns(
    rt_trips: pd.DataFrame,
    analysis_date: str,
    group_cols: list = ["trip_instance_key"]) -> pd.DataFrame:
    """
    Merge RT trips (vehicle positions) to scheduled trips.
    Add in the needed scheduled trip columns to take 
    route-direction-time_of_day averages.
    """
    keep_cols = [
        "gtfs_dataset_key",
        "direction_id", 
        "route_id", "route_short_name", "route_long_name", "route_desc",
    ] + group_cols
        
    crosswalk = helpers.import_scheduled_trips(
        analysis_date, 
        columns = keep_cols, 
        get_pandas = True
    )
    
    common_shape = sched_rt_utils.most_common_shape_by_route_direction(analysis_date)
    
    crosswalk2 = pd.merge(
        crosswalk,
        common_shape,
        on = ["schedule_gtfs_dataset_key", "route_id", "direction_id"],
        how = "inner"
    ).astype({"direction_id": "Int64"})
    
    time_of_day = sched_rt_utils.get_trip_time_buckets(analysis_date)
    
    # Clean up route name
    crosswalk2 = portfolio_utils.add_route_name(
        crosswalk2
    ).drop(columns = ["route_short_name", "route_long_name", "route_desc"])

    df = pd.merge(
        rt_trips,
        crosswalk2,
        on = group_cols,
        how = "left",
    ).merge(
        time_of_day,
        on = group_cols,
        how = "left"
    )
    
    return df


def avg_trip_speeds_with_time_of_day(
    analysis_date: str,
    dict_inputs: dict,
) -> pd.DataFrame:
    """    
    Get trip-level speeds, scheduled trip service_minutes
    and rt trip approximated-service_minutes.
    """
    start = datetime.datetime.now()
    
    SPEEDS_FILE = f'{dict_inputs["stage4"]}_{analysis_date}'
    EXPORT_FILE = f'{dict_inputs["stage6"]}_{analysis_date}'
    MAX_SPEED = dict_inputs["max_speed"]
    
    by_segment = pd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_FILE}.parquet",
        columns = ["trip_instance_key", "meters_elapsed", "sec_elapsed"]
    )
    
    by_trip = (by_segment.groupby("trip_instance_key")
           .agg({
               "meters_elapsed": "sum",
                "sec_elapsed": "sum"
                })
           .reset_index()
          )
    
    by_trip = by_trip.assign(
        speed_mph = (by_trip.meters_elapsed.divide(by_trip.sec_elapsed)
                    ) * rt_utils.MPH_PER_MPS,
        rt_trip_min = by_trip.sec_elapsed.divide(60)
    ).query('speed_mph <= @MAX_SPEED')
    
    
    df = add_scheduled_trip_columns(
        by_trip,
        analysis_date,
        group_cols = ["trip_instance_key"]        
    )
    
    df.to_parquet(
        f"{SEGMENT_GCS}trip_summary/{EXPORT_FILE}.parquet"
    )
    
    end = datetime.datetime.now()
    logger.info(f"trip summary execution time: {end - start}")

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
        logger.info(f"Analysis date: {analysis_date}")
        
        speeds_with_segment_geom(
            analysis_date, 
            STOP_SEG_DICT
        )
        
        avg_trip_speeds_with_time_of_day(
            analysis_date, 
            STOP_SEG_DICT
        )
    
