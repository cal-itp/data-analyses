"""
Convert stop-to-stop arrivals into speeds.
"""
import datetime
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers, gtfs_schedule_wrangling, segment_calcs
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH

def attach_operator_natural_identifiers(
    df: pd.DataFrame, 
    analysis_date: str
) -> pd.DataFrame:
    """
    For each gtfs_dataset_key-shape_array_key combination,
    re-attach the natural identifiers.
    Return a df with all the identifiers we need during downstream 
    aggregations, such as by route-direction.
    """
    # Get shape_id back
    shape_identifiers = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", 
                   "shape_array_key", "shape_id", 
                   "route_id", "direction_id"],
        get_pandas = True
    )
    
    # Add time-of-day, which is associated with trip_instance_key
    sched_time_of_day = gtfs_schedule_wrangling.get_trip_time_buckets(
        analysis_date
    ).rename(
        columns = {"time_of_day": "schedule_time_of_day"})
    
    # If trip isn't in schedule, use vp to derive
    vp_time_of_day = gtfs_schedule_wrangling.get_vp_trip_time_buckets(
        analysis_date
    ).rename(
        columns = {"time_of_day": "vp_time_of_day"})
    
    
    trip_used_for_shape = pd.read_parquet(
        f"{SEGMENT_GCS}segment_options/"
        f"shape_stop_segments_{analysis_date}.parquet",
        columns = ["st_trip_instance_key"]
    ).st_trip_instance_key.unique()
    
    stop_pair = helpers.import_scheduled_stop_times(
        analysis_date,
        filters = [[("trip_instance_key", "in", trip_used_for_shape)]],
        columns = ["shape_array_key", "stop_sequence", "stop_pair"],
        with_direction = True,
        get_pandas = True
    )
    
    df_with_natural_ids = pd.merge(
        df,
        shape_identifiers,
        on = "shape_array_key",
        how = "inner"
    ).merge(
        stop_pair,
        on = ["shape_array_key", "stop_sequence"]
    ).merge(
        sched_time_of_day,
        on = "trip_instance_key",
        how = "left"
    ).merge(
        vp_time_of_day,
        on = "trip_instance_key",
        how = "inner"
    )
    
    df_with_natural_ids = df_with_natural_ids.assign(
        time_of_day = df_with_natural_ids.schedule_time_of_day.fillna(
            df_with_natural_ids.vp_time_of_day)
    ).drop(columns = ["schedule_time_of_day", "vp_time_of_day"])
        
    del df, stop_pair, sched_time_of_day, vp_time_of_day
    
    return df_with_natural_ids


def calculate_speed_from_stop_arrivals(
    analysis_date: str, 
    dict_inputs: dict
):
    """
    Calculate speed between the interpolated stop arrivals of 
    2 stops. Use current stop to subsequent stop, to match
    with the segments cut by gtfs_segments.create_segments
    """
    
    STOP_ARRIVALS_FILE = f"{dict_inputs['stage3']}_{analysis_date}"
    SPEED_FILE = f"{dict_inputs['stage4']}_{analysis_date}"
        
    start = datetime.datetime.now()
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}.parquet"
    )
    
    trip_cols = ["trip_instance_key"]
    trip_stop_cols = trip_cols + ["stop_sequence"]

    df = segment_calcs.convert_timestamp_to_seconds(
        df, ["arrival_time"]
    ).sort_values(trip_stop_cols).reset_index(drop=True)
    
    df = df.assign(
        subseq_arrival_time_sec = (df.groupby(trip_cols, 
                                             observed=True, group_keys=False)
                                  .arrival_time_sec
                                  .shift(-1)
                                 ),
        subseq_stop_meters = (df.groupby(trip_cols, 
                                        observed=True, group_keys=False)
                             .stop_meters
                             .shift(-1)
                            )
    )

    speed = df.assign(
        meters_elapsed = df.subseq_stop_meters - df.stop_meters, 
        sec_elapsed = df.subseq_arrival_time_sec - df.arrival_time_sec,
    ).pipe(
        segment_calcs.derive_speed, 
        ("stop_meters", "subseq_stop_meters"), 
        ("arrival_time_sec", "subseq_arrival_time_sec")
    ).pipe(
        attach_operator_natural_identifiers, 
        analysis_date
    )
        
    speed.to_parquet(
        f"{SEGMENT_GCS}{SPEED_FILE}.parquet")
    
    end = datetime.datetime.now()
    logger.info(f"speeds by segment: {analysis_date}: {end - start}")

    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/speeds_by_segment_trip.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
        
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    for analysis_date in analysis_date_list:
        calculate_speed_from_stop_arrivals(analysis_date, STOP_SEG_DICT)
