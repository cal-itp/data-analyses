"""
Download dimensional tables for GTFS schedule
"""
import pandas as pd

from calitp_data_analysis.tables import tbls
from siuba import *

from utils import subset_feeds

def find_feeds(operator_list: list) -> dict:
    """
    Find the feed_keys for operators of interest,
    return a dictionary mapping feed_keys to gtfs_dataset_name.
    """
    df = pd.read_parquet(
        "daily_feeds.parquet",
        filters = [[("gtfs_dataset_name", "in", operator_list)]],
    )

    last_feed = pd.merge(
        df,
        df.groupby(["gtfs_dataset_name"]).agg({"date": "max"}).reset_index(),
        on = ["gtfs_dataset_name", "date"],
        how = "inner"
    )
    
    feed_to_name_dict = dict(
        zip(last_feed.feed_key, last_feed.gtfs_dataset_name)
    )
    
    return feed_to_name_dict

    
if __name__ == "__main__":
    
    (tbls.mart_gtfs.fct_daily_schedule_feeds()
        >> collect()
    ).to_parquet("daily_feeds.parquet")

    feed_to_name_dict = find_feeds(utils.la_operators)
    print(feed_to_name_dict)
    
    (tbls.mart_gtfs.dim_calendar()
        >> filter(_.feed_key.isin(subset_feeds))
        >> collect()
    ).to_parquet("dim_calendar.parquet")

    (tbls.mart_gtfs.dim_trips()
        >> filter(_.feed_key.isin(subset_feeds))
        >> collect()
    ).to_parquet("dim_trips.parquet")

    (tbls.mart_gtfs.dim_routes()
        >> filter(_.feed_key.isin(subset_feeds))
        >> collect()
    ).to_parquet("dim_routes.parquet")


