import datetime as dt

import geopandas as gpd
import pandas as pd

from .evaluate_feeds import (
    attach_transit_services,
    get_feeds_check_service,
    get_undefined_feeds,
    merge_old_feeds,
    report_unavailable_feeds,
)
from .match_feeds_regions import join_stops_regions


def get_feed_info(
    target_date: str,
    lookback_period: dt.timedelta | None = None,
    filter_geometry: gpd.GeoDataFrame | None = None,
    filter_geometry_id: str | None = None,
    report_unavailable: bool = False,
) -> pd.DataFrame:
    """
    Get a DataFrame containing feeds, filtered by region. Can be passed to `download_feeds` and `download_feeds_region` to download feeds.

    Params:
    - target_date: the date to search for feeds on. Should be formatted as a string in YYYY-MM-DD.
    - lookback_period: the time before the period to look for feeds in, if no feed is valid on `target_date`.
    - `filter_geometry`: a GDF containing geometry to spatially filter by. If this is `None`, no filter will be added.
    - `filter_geometry_id`: a column of `filter_geometry` to tag rows by. if this is not `None`, the output DF will have an additional column `region`.
    - `report_unavailable`: whether a report of feeds that were not foudn on target_date should be created and saved to "./no_apparent_service.csv"

    Returns: A DataFrame with at least the following columns
      - `date`: The most recent date within lookback_period on which a valid feed is available Will be equal to `target_date` if `lookback_period` is false
      - `feed_timezone`: the timezone associated with the feed
      - `base64_url`: the url associated with the feed in airtable
      - `gtfs_dataset_key`: the key associated with the gtfs schedule dataset associated with the feed
      - `gtfs_dataset_name`: the name associated with the gtfs schedule
      - `region`: If `filter_geometry_id` is provided, this contains that id or the key to the dict, resp.
    """
    # Get feeds DF
    feeds_on_target = get_feeds_check_service(target_date)
    feeds_on_target = attach_transit_services(feeds_on_target, target_date)

    # Get lookback feeds
    if lookback_period is not None:
        undefined_feeds = get_undefined_feeds(feeds_on_target)
        feeds_merged = merge_old_feeds(
            feeds_on_target, undefined_feeds, dt.date.fromisoformat(target_date), lookback_period
        )
    else:
        feeds_merged = feeds_on_target.copy()
    if report_unavailable:
        report_unavailable_feeds(feeds_merged, "no_apparent_service.csv")
    # Filter by region
    if filter_geometry is not None:
        if filter_geometry_id is not None:
            filter_geometry_dropped_columns = filter_geometry[
                [filter_geometry_id, filter_geometry.geometry.name]
            ].rename(columns={filter_geometry_id: "region"})
        else:
            filter_geometry_dropped_columns = filter_geometry[[filter_geometry.geometry.name]].copy()
        regions_and_feeds = join_stops_regions(
            filter_geometry_dropped_columns, feeds_merged, region_name=filter_geometry_id
        )
        regions_and_feeds_merged = regions_and_feeds.merge(
            feeds_merged[["feed_key", "gtfs_dataset_name", "base64_url", "date"]],
            how="inner",
            on="feed_key",
        )
        return regions_and_feeds_merged

    return feeds_merged
