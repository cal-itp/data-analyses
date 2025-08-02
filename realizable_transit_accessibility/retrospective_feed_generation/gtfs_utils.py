from gtfslite import GTFS
import pandas as pd
import datetime as dt
from .constants import ARBITRARY_SERVICE_ID, GTFS_DATE_STRFTIME
import copy

def subset_schedule_feed_to_one_date(feed: GTFS, service_date: dt.datetime) -> GTFS:
    """Update a gtfslite feed object to only contain service on a specified service date"""
    assert feed.valid_date(service_date), f"Feed not valid on {service_date.isoformat()}"
    # Define a new calendar dates, since the synthetic feed will only be valid on the service date
    new_calendar_dates = pd.DataFrame(
        {
            "service_id": [ARBITRARY_SERVICE_ID],
            "date": [service_date.strftime(GTFS_DATE_STRFTIME)],
            "exception_type": [1]
        },
        index=[0]
    )
    # Get only trips on the calendar date, and update their service id to match the new_calendar_dates
    trips_on_service_date = feed.date_trips(service_date).reset_index(drop=True)    
    trips_on_service_date["service_id"] = ARBITRARY_SERVICE_ID
    # Get only stop_times on the calendar date
    stop_times_on_service_date = feed.stop_times.loc[
        feed.stop_times["trip_id"].isin(trips_on_service_date["trip_id"]) # check if this is slow
    ].reset_index(drop=True)
    #TODO: evaluate whether it is necessary to remove stops, shapes, and transfers that do not have service
    #TODO: add any additional behavior for feeds with frequencies.txt
    #TODO: update feed_info.txt
    # Copy the feed, and update it to only be valid on the service date
    schedule_feed_service_date_only = copy.deepcopy(feed)
    schedule_feed_service_date_only.calendar_dates = new_calendar_dates.copy()
    schedule_feed_service_date_only.calendar = None
    schedule_feed_service_date_only.trips = trips_on_service_date
    schedule_feed_service_date_only.stop_times = stop_times_on_service_date
    return schedule_feed_service_date_only

def time_string_to_time_since_midnight(time_str_series: pd.Series) -> pd.Series:
    """
    Convert a series of strings representing GTFS format time to an series of
    ints representing seconds since midnight on the service date. 
    Will give incorrect results on days where a DST transition occurs.
    """
    return time_str_series.str.split(":").map(
        lambda s: int(s[0]) * 3600 + int(s[1]) * 60 + int(s[2])
    )
    
def seconds_to_gtfs_format_time(time_column: pd.Series) -> pd.Series:
    """Convert time in seconds since midnight (from the warehouse) to gtfs format time"""
    #TODO: this will not handle dst correctly
    hours = (time_column // 3600).astype(int).astype(str).str.rjust(width=2, fillchar="0")
    minutes = ((time_column % 3600) // 60).astype(int).astype(str).str.rjust(width=2, fillchar="0")
    seconds = (time_column % 60).astype(int).astype(str).str.rjust(width=2, fillchar="0")
    formatted = hours + ":" + minutes + ":" + seconds
    return formatted