from shared_utils import catalog_utils, rt_dates

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

last_year = rt_dates.DATES["nov2024"]
previous_month = rt_dates.DATES["oct2025"]
analysis_month = rt_dates.DATES["nov2025"]
file_name = analysis_month.replace("-","_")[0:7]

SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS
SCHED_GCS = GTFS_DATA_DICT.gcs_paths.SCHED_GCS
SHARED_GCS = GTFS_DATA_DICT.gcs_paths.SHARED_GCS