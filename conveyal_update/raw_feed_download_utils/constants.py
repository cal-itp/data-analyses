import datetime as dt

LOOKBACK_TIME = dt.timedelta(days=60)
REGIONAL_SUBFEED_NAME = "Regional Subfeed"
INT_TO_GTFS_WEEKDAY = {
    0: "monday",
    1: "tuesday",
    2: "wednesday",
    3: "thursday",
    4: "friday",
    5: "saturday",
    6: "sunday"
}