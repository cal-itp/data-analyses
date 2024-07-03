import datetime
import pandas as pd
import numpy as np
from segment_speed_utils import helpers, time_series_utils
from shared_utils import catalog_utils, rt_dates

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
from segment_speed_utils.project_vars import (COMPILED_CACHED_VIEWS, RT_SCHED_GCS, SCHED_GCS)

"""
Datasets that are relevant to
GTFS Digest Portfolio work only.
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
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
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
    # Combine all the days' data for a week
    df = concatenate_trips(date_list)
    
     # Filter
    # df = df.loc[df.name == name].reset_index(drop=True)
    
    # Add day type aka Monday, Tuesday, Wednesday...
    df['day_type'] = df['service_date'].apply(get_day_type)
    
    # Tag if the day is a weekday, Saturday, or Sunday
    df["weekend_weekday"] = df.apply(weekday_or_weekend, axis=1)
    
    # Find the minimum departure hour
    df["departure_hour"] = df.trip_first_departure_datetime_pacific.dt.hour
    
    # Delete out the specific day, leave only month & year
    df["month"] = df.service_date.astype(str).str.slice(stop=7)
    
    df2 = (
        df.groupby(["name", "month", "weekend_weekday", "departure_hour"])
        .agg(
            {
                "service_hours": "sum",
            }
        )
        .reset_index()
    )
    df2["weekday_service_hours"] = df2.service_hours/5
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

def load_operator_profiles()->pd.DataFrame:
    """
    Load operator profile dataset for one operator
    """
    op_profiles_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_profiles}.parquet"
    
    ntd_cols = [
        "schedule_gtfs_dataset_key",
        "counties_served",
        "service_area_sq_miles",
        "hq_city",
        "uza_name",
        "service_area_pop",
        "organization_type",
        "primary_uza",
        "reporter_type"
    ]
    
    # Load NTD through the crosswalk for the most recent date
    most_recent_date = rt_dates.y2024_dates[-1]
    
    ntd_df = helpers.import_schedule_gtfs_key_organization_crosswalk(most_recent_date)[
    ntd_cols]
    
    op_profiles_df = pd.read_parquet(op_profiles_url)
    
    # Keep only the most recent row
    op_profiles_df1 = (op_profiles_df
                       .sort_values(by = ['service_date'], ascending = False)
                       .drop_duplicates(subset = ["schedule_gtfs_dataset_key",
                                                  "name"])
                      ).reset_index(drop = True)
    
    # Merge
    op_profiles_df1 = pd.merge(op_profiles_df1, ntd_df, on = ["schedule_gtfs_dataset_key"], how = "left")
    
    return op_profiles_df1

if __name__ == "__main__":
    
    # Save to GCS
    OP_PROFILE_EXPORT = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_profile_portfolio_view}.parquet"
    SERVICE_EXPORT = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.scheduled_service_hours}.parquet"
    start = datetime.datetime.now()
    
    # Save operator profiles with NTD
    operator_profiles = load_operator_profiles()
    operator_profiles.to_parquet(OP_PROFILE_EXPORT)
    
    # Save service hours
    service_hours = total_service_hours_all_months()
    service_hours.to_parquet(SERVICE_EXPORT) 
    
    end = datetime.datetime.now()
    print(f"GTFS Digest Datasets: {end - start}")
