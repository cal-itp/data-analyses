from shared_utils import rt_dates

analysis_date = rt_dates.DATES["feb2023"]

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
CACHED_VIEWS_EXPORT_PATH = f"{GCS_FILE_PATH}rt_delay/cached_views/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"
SEGMENT_GCS = f"{GCS_FILE_PATH}rt_segment_speeds/"
TRAFFIC_OPS_GCS = f"{GCS_FILE_PATH}traffic_ops/"

