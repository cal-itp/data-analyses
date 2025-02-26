"""
Cached dates available in rt_delay/.

GCS: gs://calitp-analytics-data/data-analyses/rt_delay/cached_views/
"""
from typing import Literal

# HQTAs and RT speedmaps
DATES = {
    "feb2022": "2022-02-08",
    "mar2022": "2022-03-30",  # "2022-03-23"?
    # "apr2022": "", # None
    "may2022": "2022-05-04",
    "jun2022": "2022-06-15",
    "jul2022": "2022-07-13",
    "aug2022": "2022-08-17",
    "sep2022": "2022-09-14",
    "sep2022a": "2022-09-21",  # start of most v2 fct_vehicle_locations data? extent of backfill?
    "oct2022": "2022-10-12",
    "nov2022a": "2022-11-07",  # postfixed dates for d4/MTC analysis
    "nov2022b": "2022-11-08",
    "nov2022c": "2022-11-09",
    "nov2022d": "2022-11-10",
    "nov2022": "2022-11-16",
    "dec2022": "2022-12-14",
    "jan2023": "2023-01-18",  # start of v2
    "feb2023": "2023-02-15",
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
    # "aug2023": "2023-08-16", # this date is missing Muni
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
}

y2023_dates = [
    v for k, v in DATES.items() if k.endswith("2023") and not any(substring in k for substring in ["jan", "feb"])
]

y2024_dates = [v for k, v in DATES.items() if k.endswith("2024") and k not in ["oct2024g"]]
y2025_dates = [v for k, v in DATES.items() if k.endswith("2025")]


valid_weeks = ["apr2023", "oct2023", "apr2024", "oct2024"]


def get_week(month: Literal[[*valid_weeks]], exclude_wed: bool) -> list:
    if exclude_wed:
        return [v for k, v in DATES.items() if month in k and not k.endswith(month)]
    else:
        return [v for k, v in DATES.items() if month in k]


apr2023_week = get_week(month="apr2023", exclude_wed=False)
oct2023_week = get_week(month="oct2023", exclude_wed=False)
apr2024_week = get_week(month="apr2024", exclude_wed=False)
oct2024_week = [d for d in get_week(month="oct2024", exclude_wed=False) if d != DATES["oct2024g"]]

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
