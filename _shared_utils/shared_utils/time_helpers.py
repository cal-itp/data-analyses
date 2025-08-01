"""
Helpers for defining peak vs offpeak periods and
weekend and weekends so we can aggregate our
existing time-of-day bins.
"""
import datetime

import pandas as pd

PEAK_PERIODS = ["AM Peak", "PM Peak"]

HOURS_BY_TIME_OF_DAY = {
    "Owl": 4,  # [0, 3]
    "Early AM": 3,  # [4, 6]
    "AM Peak": 3,  # [7, 9]
    "Midday": 5,  # [10, 14]
    "PM Peak": 5,  # [15, 19]
    "Evening": 4,  # [20, 23]
}

TIME_OF_DAY_DICT = {
    **{k: "peak" for k, v in HOURS_BY_TIME_OF_DAY.items() if k in PEAK_PERIODS},
    **{k: "offpeak" for k, v in HOURS_BY_TIME_OF_DAY.items() if k not in PEAK_PERIODS},
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
    **{k: "weekday" for k in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]},
    **{k: "weekend" for k in ["Saturday", "Sunday"]},
}

WEEKDAY_DICT2 = {
    **{k: "Weekday" for k in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]},
    **{k: k for k in ["Saturday", "Sunday"]},
}


def time_span_labeling(date_list: list) -> tuple[str]:
    """
    If we grab a week's worth of trips, we'll
    use this week's average to stand-in for the entire month.
    Label with month and year.
    """
    time_span_str = list(set([datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%b%Y").lower() for d in date_list]))

    time_span_num = list(set([datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%m_%Y").lower() for d in date_list]))

    if len(time_span_str) == 1:
        return time_span_str[0], time_span_num[0]

    else:
        print(f"multiple months: {time_span_str}")
        return time_span_str, time_span_num


def add_time_span_columns(df: pd.DataFrame, time_span_num: str) -> pd.DataFrame:
    """
    Add columns for month / year, use when we have aggregated time-series.
    """
    month = int(time_span_num.split("_")[0])
    year = int(time_span_num.split("_")[1])

    # Downgrade some dtypes for public bucket
    df = df.assign(
        month=month,
        year=year,
    ).astype(
        {
            "month": "int16",
            "year": "int16",
        }
    )

    return df


def add_service_date(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Add a service date column for GTFS data.
    Pipe this function when we want to use dask_utils.
    """
    df = df.assign(service_date=pd.to_datetime(date))
    return df


def add_quarter(df: pd.DataFrame, date_col: str = "service_date") -> pd.DataFrame:
    """
    Parse a date column for the year, quarter it is in.
    Pipe this function when we want to use dask_utils.
    """
    df = df.assign(
        year=df[date_col].dt.year,
        quarter=df[date_col].dt.quarter,
    )

    df = df.assign(year_quarter=df.year.astype(str) + "_Q" + df.quarter.astype(str))

    return df


def categorize_time_of_day(value: int | datetime.datetime) -> str:
    if isinstance(value, int):
        hour = value
    elif isinstance(value, datetime.datetime):
        hour = value.hour
    if hour < 4:
        return "Owl"
    elif hour < 7:
        return "Early AM"
    elif hour < 10:
        return "AM Peak"
    elif hour < 15:
        return "Midday"
    elif hour < 20:
        return "PM Peak"
    else:
        return "Evening"
