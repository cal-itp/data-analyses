"""
Cached dates available in rt_delay/.

GCS: gs://calitp-analytics-data/data-analyses/rt_delay/cached_views/
"""
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
}

y2023_dates = [
    v for k, v in DATES.items() if k.endswith("2023") and not any(substring in k for substring in ["jan", "feb"])
]

y2024_dates = [v for k, v in DATES.items() if k.endswith("2024")]

apr_week = [v for k, v in DATES.items() if "apr2023" in k]
oct_week = [v for k, v in DATES.items() if "oct2023" in k]


# Planning and Modal Advisory Committee (PMAC) - quarterly
PMAC = {
    "Q1_2022": "2022-02-08",
    "Q2_2022": "2022-05-04",
    "Q3_2022": "2022-08-17",
    "Q4_2022": "2022-10-12",
    "Q1_2023": "2023-01-18",
    "Q2_2023": "2023-04-12",
    "Q3_2023": "2023-07-12",
    "Q4_2023": "2023-10-11",
}

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
