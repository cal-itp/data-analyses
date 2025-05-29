"""
Download dimensional tables for GTFS schedule
"""
import pandas as pd

from calitp_data_analysis.tables import tbls
from siuba import *

from utils import subset_feeds

if __name__ == "__main__":
    
    daily_feeds = (
        tbls.mart_gtfs.fct_daily_schedule_feeds()
        >> collect()
    )

    daily_feeds.to_parquet("daily_feeds.parquet")

    dim_schedule_feeds = (
        tbls.mart_gtfs.dim_schedule_feeds()
        >> filter(_["key"].isin(subset_feeds))
        >> collect()
    )

    dim_schedule_feeds.to_parquet("dim_schedule_feeds.parquet")

    dim_calendar = (
        tbls.mart_gtfs.dim_calendar()
        >> filter(_["feed_key"].isin(subset_feeds))
        >> collect()
    )

    dim_calendar.to_parquet("dim_calendar.parquet")

    dim_calendar_dates = (
        tbls.mart_gtfs.dim_calendar_dates()
        >> filter(_["feed_key"].isin(subset_feeds))
        >> collect()
    )

    dim_calendar_dates.to_parquet("dim_calendar_dates.parquet")

    dim_trips = (
        tbls.mart_gtfs.dim_trips()
        >> filter(_["feed_key"].isin(subset_feeds))
        >> collect()
    )

    dim_trips.to_parquet("dim_trips.parquet")

    dim_routes = (
        tbls.mart_gtfs.dim_routes()
        >> filter(_["feed_key"].isin(subset_feeds))
        >> collect()
    )

    dim_routes.to_parquet("dim_routes.parquet")


