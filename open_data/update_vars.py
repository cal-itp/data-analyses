from shared_utils import rt_dates

analysis_date = rt_dates.DATES["feb2023"]

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
TRAFFIC_OPS_GCS = f"{GCS_FILE_PATH}traffic_ops/"
HQTA_GCS = f"{GCS_FILE_PATH}high_quality_transit_areas/"
SEGMENT_GCS = f"{GCS_FILE_PATH}rt_segment_speeds/"