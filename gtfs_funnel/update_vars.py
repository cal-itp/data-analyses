from shared_utils import catalog_utils, rt_dates

apr2025_week = rt_dates.get_week("apr2025", exclude_wed=True)
oct2024_week = rt_dates.get_week("oct2024", exclude_wed=True)
apr2024_week = rt_dates.get_week("apr2024", exclude_wed=True)
oct2023_week = rt_dates.get_week("oct2023", exclude_wed=True)
apr2023_week = rt_dates.get_week("apr2023", exclude_wed=True)

all_dates = (
    oct2023_week + apr2023_week + 
    apr2024_week + oct2024_week + apr2025_week
)

analysis_date_list = [rt_dates.DATES['oct2025b']]


GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

COMPILED_CACHED_VIEWS = GTFS_DATA_DICT.gcs_paths.COMPILED_CACHED_VIEWS
SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS
SCHED_GCS = GTFS_DATA_DICT.gcs_paths.SCHED_GCS
SHARED_GCS = GTFS_DATA_DICT.gcs_paths.SHARED_GCS

PUBLISHED_OPERATORS_YAML = "published_operators.yml"
