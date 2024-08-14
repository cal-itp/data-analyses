import pandas as pd
import numpy as np
from segment_speed_utils import helpers, time_series_utils, gtfs_schedule_wrangling
from segment_speed_utils.project_vars import (COMPILED_CACHED_VIEWS, RT_SCHED_GCS, SCHED_GCS)

from shared_utils import catalog_utils, rt_dates

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

"""
Finding the total number of scheduled service hours for 
an operator across its routes for a full week. The data is
downloaded every 1/2 a year. 

Grain is operator-service_date-route
"""
def concatenate_trips(
    date_list: list,
) -> pd.DataFrame:
    """
    Concatenate schedule data that's been
    aggregated to route-direction-time_period for
    multiple days.
    """
    FILE = GTFS_DATA_DICT.schedule_downloads.trips

    df = (
        time_series_utils.concatenate_datasets_across_dates(
            COMPILED_CACHED_VIEWS,
            FILE,
            date_list,
            data_type="df",
            columns=[
                "name",
                "service_date",
                "route_long_name",
                "trip_first_departure_datetime_pacific",
                "service_hours",
            ],
        )
        .sort_values(["service_date"])
        .reset_index(drop=True)
    )

    return df

def get_day_type(date):
    """
    Function to return the day type (e.g., Monday, Tuesday, etc.) from a datetime object.
    """
    days_of_week = ["Monday", 
                    "Tuesday", 
                    "Wednesday", 
                    "Thursday", 
                    "Friday", 
                    "Saturday", 
                    "Sunday"]
    return days_of_week[date.weekday()]

def weekday_or_weekend(row):
    """
    Tag if a day is a weekday or Saturday/Sunday
    """
    if row.day_type == "Sunday":
        return "Sunday"
    if row.day_type == "Saturday":
        return "Saturday"
    else:
        return "Weekday"

def total_service_hours(date_list: list) -> pd.DataFrame:
    """
    Total up service hours by departure hour, 
    month, and day type for an operator. 
    """
    # Combine all the days' data for a week.
    df = concatenate_trips(date_list)
    
    # Find day type aka Monday, Tuesday, Wednesday based on service date.
    df['day_type'] = df['service_date'].apply(get_day_type)
    
    # Tag if the day is a weekday, Saturday, or Sunday.
    df["weekday_weekend"] = df.apply(weekday_or_weekend, axis=1)
    
    # df = gtfs_schedule_wrangling.add_weekday_weekend_column(df)
    
    # Find the minimum departure hour.
    df["departure_hour"] = df.trip_first_departure_datetime_pacific.dt.hour
    
    # Delete out the specific day, leave only month & year.
    df["month"] = df.service_date.astype(str).str.slice(stop=7)
    
    # Total up service hours by weekday, Sunday, and Saturday.
    df2 = (
        df.groupby(["name", 
                    "month", 
                    "weekday_weekend", 
                    "departure_hour"])
        .agg(
            {
                "service_hours": "sum",
            }
        )
        .reset_index()
    )
    
    # For weekday hours, divide by 5.
    df2["weekday_service_hours"] = df2.service_hours/5
    
    # Rename projects.
    df2 = df2.rename(columns = {'service_hours':'weekend_service_hours'})
    return df2

def total_service_hours_all_months() -> pd.DataFrame:
    """
    Find service hours for a full week for one operator
    and for the months we have a full week's worth of data downloaded.
    As of 5/2024, we have April 2023 and October 2023.
    """
    # Grab the dataframes with a full week's worth of data. 
    apr_23week = rt_dates.get_week(month="apr2023", exclude_wed=False)
    oct_23week = rt_dates.get_week(month="oct2023", exclude_wed=False)
    apr_24week = rt_dates.get_week(month="apr2024", exclude_wed=False)
    
    # Sum up total service_hours
    apr_23df = total_service_hours(apr_23week)
    oct_23df = total_service_hours(oct_23week)
    apr_24df = total_service_hours(apr_24week)
    
    # Combine everything
    all_df = pd.concat([apr_23df, oct_23df, apr_24df])
   
    return all_df


if __name__ == "__main__":
    
    # Save service hours.
    SERVICE_EXPORT = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.scheduled_service_hours}.parquet"
    service_hours = total_service_hours_all_months()
    service_hours.to_parquet(SERVICE_EXPORT) 
    