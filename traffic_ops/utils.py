import pandas as pd

GCS_FILE_PATH = (
    "gs://calitp-analytics-data/data-analyses/"
    "traffic_ops_raw_data/"
)
RAW_GCS = f"{GCS_FILE_PATH}hov_pems/"
PROCESSED_GCS = f"{GCS_FILE_PATH}hov_pems_processed/"

peak_hours = [7, 8, 9, 15, 16, 17, 18]

def parse_for_time_components(
    df: pd.DataFrame,
    time_col: str = "time_id"
) -> pd.DataFrame:
    """
    Parse the time_id column into several components:
    year, month, weekday (as integer), and hour.
    """
    
    df2 = df.assign(
        year = pd.to_datetime(df[time_col]).dt.year,
        month = pd.to_datetime(df[time_col]).dt.month,
        # 0 = Monday; 6 = Sunday
        weekday = pd.to_datetime(df[time_col]).dt.weekday,
        # instead of day_name(), which is string, int easier to compress
        hour = pd.to_datetime(df[time_col]).dt.hour
    )
        
    return df2

def add_peak_offpeak_column(
    df: pd.DataFrame,
    hour_col: str = "hour"
) -> pd.DataFrame:
    """
    Categorize hour into peak / offpeak.
    """
    hours_in_day = range(0, 24)
    
    peak_offpeak_dict = {
        **{k: "peak" for k in peak_hours},
        **{k: "offpeak" for k in [i for i in hours_in_day 
                                  if i not in peak_hours]}
    }
    
    df = df.assign(
        peak_offpeak = df[hour_col].map(peak_offpeak_dict)
    )
    
    return df
    
def add_weekday_weekend_column(
    df: pd.DataFrame,
    weekday_col: str = "weekday"
) -> pd.DataFrame:
    """
    Categorize day of week into daytype (weekday or weekend).
    """
    
    weekday_weekend_dict = {
        **{k: "weekday" for k in [0, 1, 2, 3, 4]},
        **{k: "weekend" for k in [5, 6]},
    }
    
    df = df.assign(
        daytype = df[weekday_col].map(weekday_weekend_dict)
    )
    
    return df