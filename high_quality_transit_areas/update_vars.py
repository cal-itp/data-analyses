from shared_utils import rt_dates
import datetime as dt

analysis_date = rt_dates.DATES["jun2025"]

GCS_FILE_PATH = ("gs://calitp-analytics-data/data-analyses/"
                 "high_quality_transit_areas/")
PROJECT_CRS = "EPSG:3310"
HQTA_SEGMENT_LENGTH = 1_250 # meters
SEGMENT_BUFFER_METERS = 35 # buffer around segment to sjoin to stops
INTERSECTION_BUFFER_METERS = 152.4 # ~ 500ft
HALF_MILE_BUFFER_METERS = 804.7 # half mile
CORRIDOR_BUFFER_METERS = HALF_MILE_BUFFER_METERS - SEGMENT_BUFFER_METERS # 755 meters
EXPORT_PATH = f"{GCS_FILE_PATH}export/{analysis_date}/"

AM_PEAK = (dt.time(hour=6), dt.time(hour=9)) #  time range to calculate average peak frequencies
PM_PEAK = (dt.time(hour=15), dt.time(hour=19))
HQ_TRANSIT_THRESHOLD = 4  #  trips per hour, from statute, see README.md
MS_TRANSIT_THRESHOLD = 3  #  different statutory threshold as of October 2024

SHARED_STOP_THRESHOLD = 8 #  current rec
#  Yolobus. Separate route_id, but same route in a CW and CCW loop, drop per rule to not compare same rt with itself
#  can match on partial keys, see create_aggregate_stop_frequencies.py
ROUTE_COLLINEARITY_KEY_PARTS_TO_DROP = ['__42A_0__42B_0', '__42B_0__42A_0']

BRANCHING_OVERLAY_BUFFER = 20
AREA_MULTIPLIER = BRANCHING_OVERLAY_BUFFER * 2
TARGET_METERS_DIFFERENCE = 5000 #  5km per route
TARGET_AREA_DIFFERENCE = TARGET_METERS_DIFFERENCE * AREA_MULTIPLIER

MPO_DATA_PATH = f'{GCS_FILE_PATH}mpo_input/' #  MPO-provided planned MTS go here as 'mpo.geojson', for example 'scag.geojson'