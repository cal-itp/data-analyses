"""
Helpers for defining peak vs offpeak periods and
weekend and weekends so we can aggregate our
existing time-of-day bins.
"""
import datetime 
import pandas as pd

PEAK_PERIODS = ["AM Peak", "PM Peak"]

HOURS_BY_TIME_OF_DAY = {
    "Owl": 4, #[0, 3]
    "Early AM": 3,  #[4, 6]
    "AM Peak": 3,  #[7, 9]
    "Midday": 5,  #[10, 14]
    "PM Peak": 5, #[15, 19]
    "Evening": 4 #[20, 23]
}

TIME_OF_DAY_DICT = {
    **{k: "peak" for k, v in HOURS_BY_TIME_OF_DAY.items() 
       if k in PEAK_PERIODS},
    **{k: "offpeak" for k, v in HOURS_BY_TIME_OF_DAY.items()
      if k not in PEAK_PERIODS}
}

DAY_TYPE_DICT = {
    1: "Sunday",
    2: "Monday",
    3: "Tuesday",
    4: "Wednesday",
    5: "Thursday",
    6: "Friday",
    7: "Saturday",
}

WEEKDAY_DICT = {
    **{k: "weekday" for k in ["Monday", "Tuesday", "Wednesday",
                             "Thursday", "Friday"]},
    **{k: "weekend" for k in ["Saturday", "Sunday"]}
}

def time_span_labeling(date_list: list) -> tuple[str]: 
    """
    If we grab a week's worth of trips, we'll
    use this week's average to stand-in for the entire month.
    Label with month and year.
    """
    time_span_str = list(set(
        [datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%b%Y").lower() 
         for d in date_list]
    ))
    
    time_span_num = list(set(
        [datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%m_%Y").lower() 
         for d in date_list]
    ))    
    
    if len(time_span_str) == 1:
        return time_span_str[0], time_span_num[0]

    else:
        print(f"multiple months: {time_span_str}")
        return time_span_str, time_span_num

    
def add_time_span_columns(
    df: pd.DataFrame, 
    time_span_num: str
) -> pd.DataFrame:
    
    month = int(time_span_num.split('_')[0])
    year = int(time_span_num.split('_')[1])
    
    # Downgrade some dtypes for public bucket
    df = df.assign(
        month = month, 
        year = year,   
    ).astype({
        "month": "int16", 
        "year": "int16",
    })
    
    return df