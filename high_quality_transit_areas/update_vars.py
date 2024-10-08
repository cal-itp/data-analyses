from shared_utils import rt_dates
import datetime as dt

analysis_date = rt_dates.DATES["sep2024"]

GCS_FILE_PATH = ("gs://calitp-analytics-data/data-analyses/"
                 "high_quality_transit_areas/")
PROJECT_CRS = "EPSG:3310"
HQTA_SEGMENT_LENGTH = 1_250 # meters
SEGMENT_BUFFER_METERS = 50 # buffer around segment to sjoin to stops
HALF_MILE_BUFFER_METERS = 805 # half mile ~ 805 meters
CORRIDOR_BUFFER_METERS = HALF_MILE_BUFFER_METERS - SEGMENT_BUFFER_METERS # 755 meters
EXPORT_PATH = f"{GCS_FILE_PATH}export/{analysis_date}/"

AM_PEAK = (dt.time(hour=6), dt.time(hour=9)) #  time range to calculate average peak frequencies
PM_PEAK = (dt.time(hour=15), dt.time(hour=19))
HQ_TRANSIT_THRESHOLD = 4  #  trips per hour, from statute, see README.md
MS_TRANSIT_THRESHOLD = 3  #  different statutory threshold as of October 2024
CORR_SINGLE_ROUTE = False #  only consider frequency from a single route at each stop (not currently recommended)
BUS_MS_SINGLE_ROUTE = False #  currently matches corridors but subject to statutory interpretation, see README.md