"""
Finding the total number of scheduled service hours for 
an operator across its routes for a full week. The data is
downloaded every 1/2 a year. 

Grain is operator-service_date-route
"""
import pandas as pd

from segment_speed_utils import (gtfs_schedule_wrangling, helpers, 
                                 time_series_utils) 
from segment_speed_utils.project_vars import (
    COMPILED_CACHED_VIEWS, weeks_available)
from shared_utils import gtfs_utils_v2, publish_utils, rt_dates
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS


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


def total_service_hours(date_list: list) -> pd.DataFrame:
    """
    Total up service hours by departure hour, 
    month, and day type for an operator. 
    """
    # Combine all the days' data for a week.
    df = concatenate_trips(date_list)
    
    WEEKDAY_DICT = {
        **{k: "Weekday" for k in ["Monday", "Tuesday", "Wednesday",
                             "Thursday", "Friday"]},
        "Saturday": "Saturday",
        "Sunday": "Sunday"
    }
    
    # Find day type (Monday, Tuesday, etc), departure hour, month_year, and weekday_weekend
    df = df.assign(
        day_type = df.service_date.dt.day_name(),
        departure_hour = df.trip_first_departure_datetime_pacific.dt.hour.astype("Int64"),
        # get month_year that's 2024-04 for Apr2024 format
        month_year = (df.service_date.dt.year.astype(str) + 
                      "-" +  df.service_date.dt.month.astype(str).str.zfill(2)),
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column, WEEKDAY_DICT
    )
    
    
    # Total up hourly service hours by weekday, Sunday, and Saturday.
    df2 = (
        df.groupby(["name", 
                    "month_year", 
                    "weekday_weekend", 
                    "departure_hour"])
        .agg({"service_hours": "sum"})
        .reset_index()
    )
    
    # weekday hours should be divided by 5, while keeping sat/sun intact
    df2 = df2.assign(
        daily_service_hours = df2.apply(
            lambda x: round(x.service_hours / 5, 2) 
            if x.weekday_weekend=="Weekday"
            else round(x.service_hours, 2), axis=1
        ),
        service_hours = df2.service_hours.round(2),
    )
    
    return df2


def total_service_hours_all_months(week_list: list[list]) -> pd.DataFrame:
    """
    Find service hours for a full week for one operator
    and for the months we have a full week's worth of data downloaded.
    As of 5/2024, we have April 2023, October 2023, and April 2024.
    """   
    public_datasets = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys(
        get_df=True
    )
    public_feeds = public_datasets.gtfs_dataset_name.unique().tolist()
    
    # Combine everything
    all_df = pd.concat(
        [total_service_hours(one_week) for one_week in week_list]
    ).pipe(
        publish_utils.exclude_private_datasets, 
        col = "name", 
        public_gtfs_dataset_keys = public_feeds
    )

    return all_df


if __name__ == "__main__":
    
    print(f"Aggregating for dates: {weeks_available}")
    
    # Save service hours
    SERVICE_EXPORT = GTFS_DATA_DICT.digest_tables.scheduled_service_hours
    
    service_hours = total_service_hours_all_months(weeks_available)
    
    service_hours.to_parquet(
        f"{RT_SCHED_GCS}{SERVICE_EXPORT}.parquet"
    ) 
    