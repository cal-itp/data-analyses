"""
Convert stop-to-stop arrivals into speeds.
"""
import datetime
import pandas as pd
import sys

from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from segment_speed_utils import (helpers, 
                                 gtfs_schedule_wrangling, 
                                 segment_calcs)
from segment_speed_utils.project_vars import (SEGMENT_GCS,
                                              CONFIG_PATH,
                                              SEGMENT_TYPES)

def attach_operator_natural_identifiers(
    df: pd.DataFrame, 
    analysis_date: str,
    segment_type: Literal["stop_segments", "rt_stop_times"]
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
        columns = ["shape_array_key", "shape_id"],
        get_pandas = True
    )
    
    route_info = gtfs_schedule_wrangling.attach_scheduled_route_info(
        analysis_date
    )    
    
    df_with_natural_ids = pd.merge(
        df,
        route_info,
        on = "trip_instance_key",
        how = "inner"
    ).merge(
        shape_identifiers,
        on = "shape_array_key",
        how = "inner",
    )
    
    if segment_type == "stop_segments":
        trip_used_for_shape = pd.read_parquet(
            f"{SEGMENT_GCS}segment_options/"
            f"shape_stop_segments_{analysis_date}.parquet",
            columns = ["st_trip_instance_key"]
        ).st_trip_instance_key.unique()

        stop_pair = helpers.import_scheduled_stop_times(
            analysis_date,
            filters = [[("trip_instance_key", "in", trip_used_for_shape)]],
            columns = ["shape_array_key", "stop_sequence", 
                       "stop_pair", "stop_pair_name"],
            with_direction = True,
            get_pandas = True
        )
        
        df_with_natural_ids = df_with_natural_ids.merge(
            stop_pair,
            on = ["shape_array_key", "stop_sequence"]
        )
    
    elif segment_type == "rt_stop_times":
        stop_pair = helpers.import_scheduled_stop_times(
            analysis_date,
            columns = ["trip_instance_key", "stop_sequence", 
                       "stop_pair", "stop_pair_name"],
            with_direction = True,
            get_pandas = True
        )
        
        df_with_natural_ids = df_with_natural_ids.merge(
            stop_pair,
            on = ["trip_instance_key", "stop_sequence"]
        )
    
    return df_with_natural_ids


def calculate_speed_from_stop_arrivals(
    analysis_date: str, 
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional[Path] = CONFIG_PATH,
):
    """
    Calculate speed between the interpolated stop arrivals of 
    2 stops. Use current stop to subsequent stop, to match
    with the segments cut by gtfs_segments.create_segments
    """
    dict_inputs = helpers.get_parameters(config_path, segment_type)

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
        analysis_date, 
        segment_type
    )
        
    speed.to_parquet(
        f"{SEGMENT_GCS}{SPEED_FILE}.parquet")
    
    end = datetime.datetime.now()
    logger.info(f"speeds by segment for {segment_type} "
                f"{analysis_date}: {end - start}")
    
    
    return


if __name__ == "__main__":
    
    LOG_FILE = "../logs/speeds_by_segment_trip.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    from segment_speed_utils.project_vars import analysis_date_list, CONFIG_PATH
    
    for analysis_date in analysis_date_list:
        calculate_speed_from_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = CONFIG_PATH
        )