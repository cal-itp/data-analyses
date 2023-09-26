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
    "oct2022a": "2022-10-03",  # postfixed dates for d4/MTC analysis
    "oct2022b": "2022-10-04",
    "oct2022c": "2022-10-05",
    "oct2022d": "2022-10-06",
    "oct2022": "2022-10-12",
    "nov2022": "2022-11-16",
    "dec2022": "2022-12-14",
    "jan2023": "2023-01-18",  # start of v2
    "feb2023": "2023-02-15",
    "mar2023": "2023-03-15",
    "apr2023a": "2023-04-10",  # postfixed dates for d4/MTC analysis
    "apr2023b": "2023-04-11",
    "apr2023": "2023-04-12",  # main April date
    "apr2023c": "2023-04-13",
    "may2023": "2023-05-17",
    "jun2023": "2023-06-14",
    "jul2023": "2023-07-12",
    # "aug2023": "2023-08-16", # this date is missing Muni
    "aug2023": "2023-08-15",
    "aug2023a": "2023-08-23",  # date used for speedmaps
    "sep2023": "2023-09-13",
}


# Planning and Modal Advisory Committee (PMAC) - quarterly
PMAC = {
    "Q1_2022": "2022-02-08",
    "Q2_2022": "2022-05-04",
    "Q3_2022": "2022-08-17",
    "Q4_2022": "2022-10-12",
    "Q1_2023": "2023-01-18",
    "Q2_2023": "2023-04-12",
    "Q3_2023": "2023-07-12",
}
