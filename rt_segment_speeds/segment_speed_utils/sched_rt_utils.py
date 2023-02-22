import dask_geopandas as dg
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from shared_utils import rt_utils
from segment_speed_utils.project_vars import COMPILED_CACHED_VIEWS, PROJECT_CRS
                                              

def get_scheduled_trips(
    analysis_date: str, 
    filters: tuple = None,
    trip_cols: list = [
        "feed_key", "name", "trip_id", 
        "shape_id", "shape_array_key", 
        "route_id", "route_key", "direction_id"
    ]
) -> dd.DataFrame:
    """
    Get scheduled trips info (all operators) for single day, 
    and keep subset of columns.
    """
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet", 
        filters = filters,
        columns = trip_cols
    )
    
    return trips


def get_routelines(
    analysis_date: str, 
    filters: tuple = None,
    shape_cols: list = ["shape_array_key", "geometry"]
) -> dg.GeoDataFrame: 
    """
    Import routelines and add route_length.
    """
    shapes = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet"
        filters = filters,
        columns = shape_cols
    ).to_crs(PROJECT_CRS)
    
    return shapes


def get_scheduled_stop_times(
    analysis_date: str, 
    filters: tuple = None,
    stop_time_cols: list = None
) -> dd.DataFrame:
    """
    Get scheduled stop times.
    """
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}st_{analysis_date}.parquet", 
        filters = filters,
        columns = stop_time_cols
    )
    
    return stop_times

    
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
    trips = (get_scheduled_trips(analysis_date)
            [keep_trip_cols]
             .drop_duplicates()
            )
    
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
