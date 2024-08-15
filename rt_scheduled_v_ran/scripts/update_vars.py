from shared_utils import catalog_utils, rt_dates
from pathlib import Path

oct2023_week = rt_dates.get_week("oct2023", exclude_wed=True)
apr2023_week = rt_dates.get_week("apr2023", exclude_wed=True)
apr2024_week = rt_dates.get_week("apr2024", exclude_wed=True)

analysis_date_list = [
    rt_dates.DATES["aug2024"]]


GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS