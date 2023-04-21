"""
Assemble stop time updates with final trip updates
and bring in scheduled stop times.

For POC, if there's any metric-specific set-up, further subsetting,
deriving of columns, do it in a script for that metric so it's clear.
Post POC, identify the patterns of what metrics can be bundled together
and calculated together (important for implementation).
For POC, focus on clarity of what metric is doing / iterate on concepts.
"""
import dask.dataframe as dd
import pandas as pd

import schedule_stop_time_wrangling as wrangle_sched
from segment_speed_utils import helpers, sched_rt_utils, segment_calcs
from segment_speed_utils.project_vars import (PREDICTIONS_GCS, 
                                              analysis_date)

trip_cols = ["gtfs_dataset_key", "service_date", "trip_id"]
stop_cols = trip_cols + ["stop_id"] 


def import_stop_time_updates(
    analysis_date: str, 
    operator: str
) -> pd.DataFrame:
    """
    Import cached stop time updates table. 
    Keep only necessary columns for doing POC, and localize all
    timestamp cols to Pacific time.
    """
    keep_cols = [
        "gtfs_dataset_key", "_gtfs_dataset_name",
        "service_date", "trip_id",
        "trip_start_time",
        "_extract_ts", "trip_update_timestamp", 
        "_trip_update_message_age",
        "stop_id", "stop_sequence",
        "arrival_time_pacific", "departure_time_pacific",
        "schedule_relationship"
    ]

    df = pd.read_parquet(
        f"{PREDICTIONS_GCS}stop_time_update_{analysis_date}_"
        f"{operator}.parquet",
        columns = keep_cols
    )
    
    timestamp_cols_to_localize = ["_extract_ts", "trip_update_timestamp"]
    
    df2 = segment_calcs.localize_vp_timestamp(
        df, 
        timestamp_cols_to_localize
    ).drop(columns = timestamp_cols_to_localize)
    
    return df2


def import_final_trip_updates(
    analysis_date: str, 
    operator: str
) -> pd.DataFrame:
    """
    Import final trip updates table.
    Rename columns because these are in Pacific time already.
    """
    keep_cols = [
        "gtfs_dataset_key", 
        "trip_id", "service_date",
        "stop_id",
        "last_trip_updates_arrival_time", 
        "last_trip_updates_departure_time",
    ]
    
    df = pd.read_parquet(
        f"{PREDICTIONS_GCS}final_trip_updates_{analysis_date}_"
        f"{operator}.parquet",
        columns = keep_cols
    ).rename(columns = {
        # Rename to reflect how Google Doc is referring to it
        "last_trip_updates_arrival_time": "actual_stop_arrival_time_pacific", 
        "last_trip_updates_departure_time": "actual_stop_departure_time_pacific",
    })
    
    return df


def derive_trip_start(stop_times_df: pd.DataFrame) -> pd.DataFrame:
    """
    Based on stop_time_update table, get the trip start time. 
    Final updates has stop_id, but we lose stop_sequence, so let's get it
    from here now.
    Use first prediction for arrival time of first stop, min(stop_sequence),
    """
    # Get first stop for trip
    first_stop = (stop_times_df.groupby(trip_cols)
              .agg({"stop_sequence": "min"})
              .reset_index()
             )
         
    # Merge back to df to keep just the first stop,
    # and grab the first arrival time
    first_prediction_for_origin = (
        stop_times_df.merge(
            first_stop,
            on = trip_cols,
            how = "inner"
        ).groupby(trip_cols)
        .agg({"arrival_time_pacific": "min"})
        .reset_index()
        .rename(columns = {"arrival_time_pacific": "trip_start_time"})
    )
                 
    stop_times_with_trip_start = pd.merge(
        stop_times_df.drop(columns = "trip_start_time"),
        first_prediction_for_origin,
        on = trip_cols,
        how = "inner"
    )
    return stop_times_with_trip_start


def derive_trip_end(stop_times_df: pd.DataFrame) -> pd.DataFrame:
    """
    Use predictions to get last prediction for arrival time as 
    the trip_end, whether or not trip was scheduled or not.
    last prediction for arrival time of last stop, max(stop_sequence).
    """
    # Get last stop for trip
    last_stop = (stop_times_df.groupby(trip_cols)
                 .agg({"stop_sequence": "max"})
                 .reset_index()
                )    
    
    # Merge back to df to keep just the last stop,
    # and grab the last arrival time
    last_prediction_for_destination = (
        stop_times_df.merge(
            last_stop,
            on = trip_cols,
            how = "inner"
        ).groupby(trip_cols)
        .agg({"arrival_time_pacific": "max"})
        .reset_index()
        .rename(columns = {"arrival_time_pacific": "trip_end_time"})
    )
    
    stop_times_with_trip_end = pd.merge(
        stop_times_df,
        last_prediction_for_destination,
        on = trip_cols,
        how = "inner"
    )
    
    return stop_times_with_trip_end


def add_trip_start_end_by_sched_relationship(df: pd.DataFrame):
    scheduled = df[(df.schedule_relationship=="SCHEDULED") & 
                   (df.scheduled_trip_start.notna())
                  ]
    not_scheduled = df[(df.schedule_relationship!="SCHEDULED") | 
                       (df.scheduled_trip_start.isna())
                      ]
    
    # For scheduled trips, the trip_start should be the first
    # arrival timestamp from scheduled stop_times, whether or not
    # trip_start is filled in
    scheduled2 = scheduled.assign(
        trip_start_time = scheduled.scheduled_trip_start
    )
    
    # For not scheduled trips, we'll derive the trip start
    # using the first arrival prediction
    not_scheduled2 = derive_trip_start(
        not_scheduled)
    
    df2 = pd.concat([scheduled2, not_scheduled2], axis=0)
    
    # For scheduled and not scheduled, we want to use 
    # last prediction as trip end
    df3 = derive_trip_end(df2).drop(columns = "scheduled_trip_start")
    
    return df3


def exclude_predictions_before_trip_start(
    df: pd.DataFrame,
    timestamp_col: str,
    cutoff_minutes_prior: int = 60 # 1 hr
) -> pd.DataFrame:
    """
    Drop the rows where predictions are occurring way too early 
    before trip_start.
    
    * Update Completeness metric measured from trip start
    time til end, but we need a bit of buffer for predictions
    prior to the first stop / origin. 
    
    TODO: Eric to give feedback what's reasonable...45 min? 
    Adjust excluding to include that.
    """    
    df = df.assign(
        seconds_difference = (df.trip_start_time - 
                              df[timestamp_col]).dt.total_seconds() 
    )
    
    df2 = (df[df.seconds_difference <= cutoff_minutes_prior*60]
           .drop(columns = "seconds_difference")
           .reset_index(drop=True)
          )
    
    return df2


def exclude_predictions_after_trip_end(
    df: pd.DataFrame, 
    timestamp_col: str
) -> pd.DataFrame: 
    """
    Drop the rows where predictions are occuring after the trip has 
    already ended. 
    """
    df2 =  (df[df[timestamp_col] <= df.trip_end_time]
            .drop(columns = "trip_end_time")
            .reset_index(drop=True)
           )
    
    return df2


def get_usable_predictions(
    stop_time_updates: pd.DataFrame,
    final_updates: pd.DataFrame,
    analysis_date: str,
) -> pd.DataFrame: 
    """
    Top-level function for doing all the general pre-processing needed 
    for stop_time_updates.
    From this, calculate each metric.
    """
    scheduled_stop_times = (
        wrangle_sched.scheduled_stop_times_with_rt_dataset_key(
        analysis_date, 
    ))
    
    # Fill in schedule_relationship if it's missing
    df = wrangle_sched.derive_schedule_relationship(
        stop_time_updates, scheduled_stop_times).compute()
    
    # For scheduled trips, get scheduled trip start; 
    # For unscheduled trips, derive trip start.
    # For both, derive trip_end.
    df2 = add_trip_start_end_by_sched_relationship(df)
    
    # Decide here to use a certain timestamp column for wrangling
    timestamp_col = "_extract_ts_local"
    
    df3 = exclude_predictions_before_trip_start(
        df2, 
        timestamp_col,
        cutoff_minutes_prior = 60
    )
    
    df4 = exclude_predictions_after_trip_end(
        df3, 
        timestamp_col,
    )
    
    # Stats
    print(f"# rows: {len(df)}, # rows after dropping prior trip start: {len(df3)}, # rows after dropping after trip end: {len(df4)}")
    
    final = df4.merge(
        final_updates,
        on = stop_cols,
        how = "inner"
    )
    
    return final
