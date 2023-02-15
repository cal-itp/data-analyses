import dask_geopandas as dg
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from shared_utils import geography_utils, rt_utils
from update_vars import COMPILED_CACHED_VIEWS, analysis_date


def get_scheduled_trips(analysis_date: str) -> dd.DataFrame:
    """
    Get scheduled trips info (all operators) for single day, 
    and keep subset of columns.
    """
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    keep_cols = ["feed_key", "name",
                 "trip_id", "shape_id", "shape_array_key",
                 "route_id", "route_key", "direction_id"
                ] 
    trips = trips[keep_cols]
    
    return trips


def get_routelines(
    analysis_date: str, buffer_size: int = 50
) -> dg.GeoDataFrame: 
    """
    Import routelines (shape_ids) and add route_length and buffer by 
    some specified size (50 m to start)
    """
    routelines = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet"
    ).to_crs(geography_utils.CA_NAD83Albers)
             
    keep_cols = ["shape_array_key", "route_length", 
                 "geometry", "shape_geometry_buffered"
                ]
    
    routelines = routelines.assign(
        route_length = routelines.geometry.length,
        shape_geometry_buffered = routelines.geometry.buffer(buffer_size)
    )[keep_cols]
    
    return routelines

    
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
    fct_rt_feeds = rt_utils.get_rt_schedule_feeds_crosswalk(
        analysis_date, 
        keep_cols = ["gtfs_dataset_key", "schedule_feed_key", "feed_type"], 
        get_df = True,
        custom_filtering = {"feed_type": ["vehicle_positions"]}
    ).rename(columns = {"schedule_feed_key": "feed_key"}).drop(columns = "feed_type")
    
    # Merge trips with fct_rt_feeds to get gtfs_dataset_key
    trips_with_rt_key = dd.merge(
        trips,
        fct_rt_feeds,
        on = "feed_key",
        how = "inner"
    ).compute()
    
    return trips_with_rt_key