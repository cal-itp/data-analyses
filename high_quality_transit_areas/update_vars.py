from shared_utils import rt_dates

analysis_date = rt_dates.DATES["sep2024"]

GCS_FILE_PATH = ("gs://calitp-analytics-data/data-analyses/"
                 "high_quality_transit_areas/")
PROJECT_CRS = "EPSG:3310"
HQTA_SEGMENT_LENGTH = 1_250 # meters
SEGMENT_BUFFER_METERS = 50 # buffer around segment to sjoin to stops
HALF_MILE_BUFFER_METERS = 805 # half mile ~ 805 meters
CORRIDOR_BUFFER_METERS = HALF_MILE_BUFFER_METERS - SEGMENT_BUFFER_METERS # 755 meters
EXPORT_PATH = f"{GCS_FILE_PATH}export/{analysis_date}/"