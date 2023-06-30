"""
Collect all the utility functions related to
POC for real-time metrics.
"""
import pandas as pd


def parse_hour_min(
    df: pd.DataFrame, 
    timestamp_col: list
) -> pd.DataFrame:
    """
    Parse out the hour and minute from some 
    timestamp_column (header_timestamp, trip_update_timestamp)
    """
    for c in timestamp_col:
        df = df.assign(
            hour = df[c].dt.hour,
            minute = df[c].dt.minute
        ).rename(columns = {
            "hour": f"{c}_hour", 
            "minute": f"{c}_min"})
    
    return df


def minute_cols(timestamp_col: str) -> list:
    """
    Return list of the _hour, _min columns for grouping
    """
    return [f"{timestamp_col}_hour", f"{timestamp_col}_min"]


def exclude_predictions_after_actual_stop_arrival(
    df: pd.DataFrame,
    timestamp_col: str
) -> pd.DataFrame:
    """
    Drop the predictions within a trip. Previous exclusion 
    only dropped predictions after trip ended.
    Now, drop predictions for a stop after the stop has arrived.
    """
    df2 = df[df[timestamp_col] <= df.actual_stop_arrival_pacific
            ].reset_index(drop=True)
    
    return df2


def exclude_predictions_at_actual_stop_arrival(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    We might want to exclude predictions happening the same
    minute of the stop arrival because they're less useful to riders.
    """
    df = parse_hour_min(
        df, 
        ["_extract_ts_local", "predicted_pacific"]
    )
    
    hour_cols = [c for c in df.columns if "_hour" in c]
    minute_cols = [c for c in df.columns if "_min" in c]
    
    df2 = df[~(
        (df._extract_ts_local_hour == df.predicted_pacific_hour) & 
        (df._extract_ts_local_min == df.predicted_pacific_min)
    )].drop(columns = hour_cols + minute_cols).reset_index(drop=True)
    
    return df2


def exclude_predictions_before_trip_start_time(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    We might want to exclude predictions before the trip_start_time,
    basically, get rid of predictions happening coming in the 1 hr
    before.
    """
    df2 = df[df._extract_ts_local >= df.trip_start_time
            ].reset_index(drop=True)
    
    return df2


def set_prediction_window(
    df: pd.DataFrame, 
    min_before: int
) -> pd.DataFrame:
    """
    We can set a prediction window, such as 30 min prior to 
    actual stop arrival.
    """
    df = df.assign(
        prediction_window = (df.actual_stop_arrival_pacific - 
                             pd.Timedelta(minutes = min_before))
    )
        
    df2 = df[
        df._extract_ts_local >= df.prediction_window
    ].drop(columns = "prediction_window").reset_index(drop=True)
    
    return df2