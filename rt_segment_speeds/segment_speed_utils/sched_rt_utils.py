"""
Functions for bridging schedule and RT data.
From RT data, gtfs_dataset_key is used.
From schedule data, feed_key is used.

These functions start with schedule data and add the RT gtfs_dataset_key.
"""
import dask_geopandas as dg
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from shared_utils import rt_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import COMPILED_CACHED_VIEWS, PROJECT_CRS


def crosswalk_scheduled_trip_grouping_with_rt_key(
    analysis_date: str, 
    keep_trip_cols: list = ["feed_key", "trip_id"],
) -> pd.DataFrame:
    """
    Filter scheduled trips to a certain grouping 
    (with route_id, direction_id or shape_array_key), 
    and merge in gtfs_dataset_key that comes from fct_rt_feeds.
    
    This is our crosswalk that we can stick in the middle of vp or segments
    and that allows us to get feed_key and gtfs_dataset_key
    """
    trips = helpers.import_scheduled_trips(
        analysis_date, 
        columns = keep_trip_cols
    ).drop_duplicates()
    
    # Get the schedule feed_key and RT gtfs_dataset_key and add it to crosswalk
    fct_rt_feeds = (rt_utils.get_rt_schedule_feeds_crosswalk(
            analysis_date, 
            keep_cols = ["gtfs_dataset_key", "schedule_feed_key", "feed_type"], 
            get_df = True,
            custom_filtering = {"feed_type": ["vehicle_positions"]}
        ).rename(columns = {"schedule_feed_key": "feed_key"})
        .drop(columns = "feed_type")
    )
    
    # Merge trips with fct_rt_feeds to get gtfs_dataset_key
    trips_with_rt_key = dd.merge(
        trips,
        fct_rt_feeds,
        on = "feed_key",
        how = "inner"
    ).compute()
    
    return trips_with_rt_key


def add_rt_keys_to_segments(
    segments: gpd.GeoDataFrame, 
    analysis_date: str,
    merge_cols: list,
) -> gpd.GeoDataFrame:
    """
    Once segments are cut, add the RT gtfs_dataset_key 
    using crosswalk_scheduled_trip_grouping_with_rt_key
    function.
    """
    crosswalk = crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, 
        merge_cols
    )
    
    segments_with_crosswalk = pd.merge(
        segments,
        crosswalk,
        on = merge_cols,
        how = "inner",
    )
    
    return segments_with_crosswalk 
