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

from typing import List, Literal

from shared_utils import schedule_rt_utils, rt_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import COMPILED_CACHED_VIEWS, PROJECT_CRS


def crosswalk_scheduled_trip_grouping_with_rt_key(
    analysis_date: str, 
    keep_trip_cols: list = ["feed_key", "trip_id"],
    feed_types: List[Literal["vehicle_positions", 
                             "trip_updates", 
                             "service_alerts"]] = ["vehicle_positions"],
    **kwargs
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
        columns = keep_trip_cols,
        **kwargs
    )
    
    # Get the schedule feed_key and RT gtfs_dataset_key and add it to crosswalk
    fct_rt_feeds = (schedule_rt_utils.get_rt_schedule_feeds_crosswalk(
            analysis_date, 
            keep_cols = ["gtfs_dataset_key", "schedule_feed_key", "feed_type"], 
            get_df = True,
            custom_filtering = {"feed_type": feed_types}
        ).rename(columns = {"schedule_feed_key": "feed_key"})
        .drop(columns = "feed_type")
    )
    
    # Merge trips with fct_rt_feeds to get gtfs_dataset_key
    if isinstance(trips, dd.DataFrame):
        trips_with_rt_key = dd.merge(
            trips,
            fct_rt_feeds,
            on = "feed_key",
            how = "inner"
        )
    
    else:
        trips_with_rt_key = pd.merge(
            trips,
            fct_rt_feeds,
            on = "feed_key",
            how = "inner"
        )    
        
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


def get_trip_time_buckets(analysis_date: str) -> pd.DataFrame:
    """
    Assign trips to time-of-day.
    """
    keep_trip_cols = [
        "feed_key", "trip_id", 
        "service_hours", 
        "trip_first_departure_datetime_pacific"
    ]
    
    trips = crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, 
        keep_trip_cols,
        get_pandas = True
    )                 
                      
    trips = trips.assign(
        time_of_day = trips.apply(
            lambda x: rt_utils.categorize_time_of_day(
                x.trip_first_departure_datetime_pacific), axis=1), 
        service_minutes = trips.service_hours * 60
    )
    
    return trips


def most_common_shape_by_route_direction(analysis_date: str) -> pd.DataFrame:
    """
    Find shape_id with most trips for that route-direction.
    """
    route_dir_cols = [
        "feed_key", "route_id", "direction_id"]
    
    keep_trip_cols = route_dir_cols + [
        "trip_id", "shape_id", "shape_array_key"
    ]
    
    trips = crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, 
        keep_trip_cols,
        get_pandas = True
    )                 
    
    sorting_order = [True for i in route_dir_cols]
    
    most_common_shape = (
        trips.groupby(route_dir_cols + ["shape_id", "shape_array_key"], 
                      observed=True, group_keys = False)
        .agg({"trip_id": "count"})
        .reset_index()
        .sort_values(route_dir_cols + ["trip_id"], 
                     ascending = sorting_order + [False])
        .drop_duplicates(subset=route_dir_cols)
        .reset_index(drop=True)
        .rename(columns = {"shape_id": "common_shape_id"})
        [route_dir_cols + ["common_shape_id", "shape_array_key"]]
    )
    
    return most_common_shape
    