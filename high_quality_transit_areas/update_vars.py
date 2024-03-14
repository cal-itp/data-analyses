from shared_utils import rt_dates

analysis_date = rt_dates.DATES["mar2024"]

GCS_FILE_PATH = ("gs://calitp-analytics-data/data-analyses/"
                 "high_quality_transit_areas/")
TEMP_GCS = f"{GCS_FILE_PATH}temp/"
PROJECT_CRS = "EPSG:3310"
HQTA_SEGMENT_LENGTH = 1_250 # meters
BUFFER_METERS = 50