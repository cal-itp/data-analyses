from shared_utils import rt_dates

# constants
TARGET_DATE = rt_dates.DATES["jul2025"]
TARGET_TIME_OF_DAY = "AM Peak"
ANALYSIS_DISTRICT_NUMBER = 7
TARGET_TIME_OF_DAY_LENGTH_HOURS = (
    3  # the length of the target time of day (3 hours for am peak)
)
SJOIN_DISTANCE_METERS = 155
STUDY_CORRIDOR_BUFFER_DISTANCE = 200
SIGNAL_OWNER_NAME = "signal_owner"

STATE_OF_CALIFORNIA_LABEL = "State of California"
CITY_OF_LOS_ANGELES_LABEL = "City of Los Angeles"
CITY_OF_SANTA_MONICA_LABEL = "City of Santa Monica"

CALTRANS_INTERNAL_SIGNAL_URI = "gs://calitp-analytics-data/data-analyses/rt_delay/signals/signals_2025-09-08.geojson"
LOS_ANGELES_OPEN_SIGNAL_URI = "gs://calitp-analytics-data/data-analyses/rt_delay/signals/los_angeles_signals_2025-10-06.geojson"
SANTA_MONICA_OPEN_SIGNAL_URI = "gs://calitp-analytics-data/data-analyses/rt_delay/signals/santa_monica_signals_2025-10-06.geojson"
