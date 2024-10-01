from shared_utils import catalog_utils, rt_dates
from pathlib import Path

oct2023_week = rt_dates.get_week("oct2023", exclude_wed=True)
apr2023_week = rt_dates.get_week("apr2023", exclude_wed=True)
apr2024_week = rt_dates.get_week("apr2024", exclude_wed=True)

# analysis_date_list = [rt_dates.DATES["sep2024"]]
analysis_date_list = {k: v for k, v in your_dict.items() if (k[:3], k[3:]) >= ('mar', '2023')}

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS