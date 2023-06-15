from shared_utils import rt_dates

analysis_date = rt_dates.DATES["jun2023"]

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"
TRAFFIC_OPS_GCS = f"{GCS_FILE_PATH}traffic_ops/"
HQTA_GCS = f"{GCS_FILE_PATH}high_quality_transit_areas/"
SEGMENT_GCS = f"{GCS_FILE_PATH}rt_segment_speeds/"