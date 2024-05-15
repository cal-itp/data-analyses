from shared_utils import catalog_utils, rt_dates

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

GCS_FILE_PATH = GTFS_DATA_DICT.gcs_paths.GCS
COMPILED_CACHED_VIEWS = GTFS_DATA_DICT.gcs_paths.COMPILED_CACHED_VIEWS
SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS
SCHED_GCS = GTFS_DATA_DICT.gcs_paths.SCHED_GCS
PREDICTIONS_GCS = GTFS_DATA_DICT.gcs_paths.PREDICTIONS_GCS
SHARED_GCS = GTFS_DATA_DICT.gcs_paths.SHARED_GCS
PUBLIC_GCS = GTFS_DATA_DICT.gcs_paths.PUBLIC_GCS

analysis_date = rt_dates.DATES["apr2024"]

oct_week = rt_dates.get_week("oct2023", exclude_wed=True)
apr_week = rt_dates.get_week("apr2023", exclude_wed=True)
analysis_date_list  = [analysis_date]

PROJECT_CRS = "EPSG:3310"
CONFIG_PATH = "./config.yml"
ROAD_SEGMENT_METERS = 1_000
SEGMENT_TYPES = ["stop_segments", "rt_stop_times"]
