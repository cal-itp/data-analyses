"""
Aggregate route-direction-time_of_day speeds to route-level
and merge with categorized routes.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from shared_utils import rt_utils, schedule_rt_utils
from update_vars import BUS_SERVICE_GCS, ANALYSIS_DATE
from segment_speed_utils.project_vars import SEGMENT_GCS


def aggregate_trip_speeds_to_route(
    analysis_date: str,
    route_cols: list
):
    """
    Start with trip speeds and aggregate to route-level.
    Instead of using route_speeds (which aggregates to 
    route-direction-time_of_day and uses source_record_id),
    let's just use trip-level speeds.
    
    Also, back out the operator's median speed, and use
    that as the target speed for a given route.
    """
    
    speeds = pd.read_parquet(
        f"{SEGMENT_GCS}trip_summary/trip_speeds_{analysis_date}.parquet",
        columns = route_cols + ["trip_instance_key", "speed_mph"]
    )
    
    speeds_by_route = (speeds.groupby(route_cols, 
                                      observed=True, group_keys=False)
                       .agg({
                           "speed_mph": "mean",
                           "trip_instance_key": "count"
                       }).reset_index()
                       .rename(columns = {"trip_instance_key": "n_trips"})
                      )
    '''
    system_median = (speeds.groupby("schedule_gtfs_dataset_key", 
                                    observed=True, group_keys=False)
                     .agg({"speed_mph": "median"})
                     .reset_index()
                     .rename(columns = {"speed_mph": "system_speed_median"})
                    )
    
    speeds_by_route2 = pd.merge(
        speeds_by_route,
        system_median,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )
    '''
    
    return speeds_by_route
    
    
def speed_to_delay_estimate(
    df: gpd.GeoDataFrame, 
    target_speeds_dict: dict
) -> gpd.GeoDataFrame:
    """
    Assign target speeds using dict. Estimate delay hours based on 
    this equation:
    
    speed = distance / time
    time = distance / speed
    
    hours = miles / miles_per_hour

    Take difference between target speed and actual speed and 
    back out delay hours.
    """
    df = df.assign(
        target_speed = df.category.map(target_speeds_dict)
    )
    
    df = df.assign(
        actual_service_hours = df.route_length_mi.divide(df.speed_mph),
        target_service_hours = df.route_length_mi.divide(df.target_speed),
    )
    
    df = df.assign(
        delay_hours = (df.actual_service_hours - 
                       df.target_service_hours) * df.n_trips
    )
    
    return df      

        
if __name__=="__main__":
    
    logger.add("./logs/quarterly_performance_pipeline.log", retention = "6 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    routes_categorized = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}routes_categorized_{ANALYSIS_DATE}.parquet"
    ).rename(
        columns = {"gtfs_dataset_key": "schedule_gtfs_dataset_key"}
    )
    
    route_cols = [
        "schedule_gtfs_dataset_key",
        "route_id"
    ]
    
    speeds_by_route = aggregate_trip_speeds_to_route(
        ANALYSIS_DATE, route_cols)
    
    final = pd.merge(
        routes_categorized,
        speeds_by_route,
        on = route_cols,
        how = "left",
        validate = "1:1",
        indicator = True
    )
    
    MERGE_CATEGORIES = {
        "both": "rt_and_sched",
        "left_only": "schedule_only",
        "right_only": "rt_only"
    }
    
    final = final.assign(
        rt_sched_category = final._merge.map(MERGE_CATEGORIES)
    ).drop(columns = "_merge")
    
    TARGET_SPEEDS = {
        "on_shn": 16,
        "intersects_shn": 16,
        "other": 16,
    }
    
    final = speed_to_delay_estimate(final, TARGET_SPEEDS)  
    
    utils.geoparquet_gcs_export(
        final, 
        BUS_SERVICE_GCS,
        f"routes_categorized_with_speed_{ANALYSIS_DATE}"
    )
     
    end = datetime.datetime.now()
    logger.info(f"attach speed: {ANALYSIS_DATE}  {end - start}")