"""
Cached dates available in rt_delay/.

GCS: gs://calitp-analytics-data/data-analyses/rt_delay/cached_views/
"""
import datetime
from typing import Literal

# HQTAs and RT speedmaps
DATES = {
    # "jan2023": "2023-01-18",  # start of v2
    # "feb2023": "2023-02-15",
    "mar2023": "2023-03-15",
    "apr2023a": "2023-04-10",  # postfixed dates for d4/MTC analysis
    "apr2023b": "2023-04-11",
    "apr2023": "2023-04-12",  # main April date
    "apr2023c": "2023-04-13",
    "apr2023d": "2023-04-14",
    "apr2023e": "2023-04-15",
    "apr2023f": "2023-04-16",
    "may2023": "2023-05-17",
    "jun2023": "2023-06-14",
    "jul2023": "2023-07-12",
    "aug2023": "2023-08-15",
    "aug2023a": "2023-08-23",  # date used for speedmaps
    "sep2023": "2023-09-13",
    "oct2023a": "2023-10-09",
    "oct2023b": "2023-10-10",
    "oct2023": "2023-10-11",
    "oct2023c": "2023-10-12",
    "oct2023d": "2023-10-13",
    "oct2023e": "2023-10-14",  # add weekend dates for SB 125 service increase
    "oct2023f": "2023-10-15",
    "nov2023": "2023-11-15",
    "dec2023": "2023-12-13",
    "jan2024": "2024-01-17",
    "feb2024": "2024-02-14",
    "mar2024": "2024-03-13",
    "apr2024a": "2024-04-15",
    "apr2024b": "2024-04-16",
    "apr2024": "2024-04-17",
    "apr2024c": "2024-04-18",
    "apr2024d": "2024-04-19",
    "apr2024e": "2024-04-20",
    "apr2024f": "2024-04-21",
    "may2024": "2024-05-22",
    "jun2024": "2024-06-12",
    "jul2024": "2024-07-17",
    "aug2024": "2024-08-14",
    "sep2024": "2024-09-18",
    "oct2024a": "2024-10-14",
    "oct2024b": "2024-10-15",
    "oct2024": "2024-10-16",
    "oct2024c": "2024-10-17",
    "oct2024d": "2024-10-18",
    "oct2024e": "2024-10-19",
    "oct2024f": "2024-10-20",
    "oct2024g": "2024-10-21",  # additional one-off to capture Amtrak in HQTA
    "nov2024": "2024-11-13",
    "dec2024": "2024-12-11",
    "jan2025": "2025-01-15",
    "feb2025": "2025-02-12",
    "mar2025": "2025-03-12",
    "apr2025a": "2025-04-14",
    "apr2025b": "2025-04-15",
    "apr2025": "2025-04-16",  # main April date
    "apr2025c": "2025-04-17",
    "apr2025d": "2025-04-18",
    "apr2025e": "2025-04-19",
    "apr2025f": "2025-04-20",
    "may2025": "2025-05-14",
    "jun2025": "2025-06-11",
    "jul2025": "2025-07-16",
}

years_available = list(range(2023, datetime.datetime.now().year + 1))
current_year = max(years_available)

y2023_dates = [
    v for k, v in DATES.items() if k.endswith("2023") and not any(substring in k for substring in ["jan", "feb"])
]

y2024_dates = [v for k, v in DATES.items() if k.endswith("2024") and k not in ["oct2024g"]]
y2025_dates = [v for k, v in DATES.items() if k.endswith("2025")]

DATES_BY_YEAR_DICT = {
    2023: y2023_dates,
    2024: y2024_dates,
    2025: y2025_dates,
}

valid_weeks = ["apr2023", "oct2023", "apr2024", "oct2024", "apr2025"]


# Remove all the one-offs
one_off_dates = ["jan2023", "feb2023", "aug2023a", "oct2024g"]
all_dates = [v for k, v in DATES.items() if k not in one_off_dates and "2022" not in k]


def get_week(month: Literal[[*valid_weeks]], exclude_wed: bool) -> list:
    if exclude_wed:
        return [v for k, v in DATES.items() if (month in k) and (not k.endswith(month)) and (k not in one_off_dates)]
    else:
        return [v for k, v in DATES.items() if month in k and k not in one_off_dates]


apr2023_week = get_week(month="apr2023", exclude_wed=False)
oct2023_week = get_week(month="oct2023", exclude_wed=False)
apr2024_week = get_week(month="apr2024", exclude_wed=False)
oct2024_week = get_week(month="oct2024", exclude_wed=False)
apr2025_week = get_week(month="apr2025", exclude_wed=False)

MONTH_DICT = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}
