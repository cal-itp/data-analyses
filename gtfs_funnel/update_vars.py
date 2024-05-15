from shared_utils import catalog_utils, rt_dates

all_dates = (rt_dates.y2024_dates + rt_dates.y2023_dates + 
             rt_dates.oct_week + rt_dates.apr_week)

analysis_date_list = [
    rt_dates.DATES["apr2024"]
] 

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

COMPILED_CACHED_VIEWS = GTFS_DATA_DICT.gcs_paths.COMPILED_CACHED_VIEWS
SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS
SCHED_GCS = GTFS_DATA_DICT.gcs_paths.SCHED_GCS
SHARED_GCS = GTFS_DATA_DICT.gcs_paths.SHARED_GCS