from shared_utils import rt_dates

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"
SEGMENT_GCS = f"{GCS_FILE_PATH}rt_segment_speeds/"
RT_SCHED_GCS = f"{GCS_FILE_PATH}rt_vs_schedule/"
SCHED_GCS = f"{GCS_FILE_PATH}gtfs_schedule/"
PREDICTIONS_GCS = f"{GCS_FILE_PATH}rt_predictions/"
SHARED_GCS = f"{GCS_FILE_PATH}shared_data/"
PUBLIC_GCS = "gs://calitp-publish-data-analysis/"

analysis_date = rt_dates.DATES["dec2023"]

days = ["a", "b", "", "c", "d", "e", "f"] 
months = [
    "dec", 
    #"nov", "oct", "sep",
    #"aug", "jul", "jun",
    #"may", "apr", "mar"
]

analysis_date_list = [
    rt_dates.DATES[f"{m}2023"] for m in months
]

PROJECT_CRS = "EPSG:3310"
CONFIG_PATH = "./config.yml"
ROAD_SEGMENT_METERS = 1_000