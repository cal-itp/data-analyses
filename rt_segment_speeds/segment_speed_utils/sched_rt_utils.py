"""
Functions for bridging schedule and RT data.

RT and schedule trips are joined using trip_instance_key.
https://github.com/cal-itp/data-infra/pull/2489

"""
import dask_geopandas as dg
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from shared_utils import rt_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import COMPILED_CACHED_VIEWS, PROJECT_CRS


def get_trip_time_buckets(analysis_date: str) -> pd.DataFrame:
    """
    Assign trips to time-of-day.
    """
    keep_trip_cols = [
        "trip_instance_key", 
        "service_hours", 
        "trip_first_departure_datetime_pacific"
    ]
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = keep_trip_cols,
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
    route_dir_cols = ["gtfs_dataset_key", "route_id", "direction_id"]
    
    keep_trip_cols = route_dir_cols + [
        "trip_instance_key", "shape_id", "shape_array_key"
    ]
    
    trips = helpers.import_scheduled_trips(
        analysis_date, 
        columns = keep_trip_cols,
        get_pandas = True
    ).rename(columns = {"schedule_gtfs_dataset_key": "gtfs_dataset_key"})                 
    
    sorting_order = [True for i in route_dir_cols]
    
    most_common_shape = (
        trips.groupby(route_dir_cols + ["shape_id", "shape_array_key"], 
                      observed=True, group_keys = False)
        .agg({"trip_instance_key": "count"})
        .reset_index()
        .sort_values(route_dir_cols + ["trip_instance_key"], 
                     ascending = sorting_order + [False])
        .drop_duplicates(subset=route_dir_cols)
        .reset_index(drop=True)
        [route_dir_cols + ["shape_id", "shape_array_key"]]
    ).rename(columns = {
        "gtfs_dataset_key": "schedule_gtfs_dataset_key", 
        "shape_id": "common_shape_id"
    })  
    
    return most_common_shape
    