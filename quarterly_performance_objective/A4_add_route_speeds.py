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
from segment_speed_utils import gtfs_schedule_wrangling
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT
from update_vars import BUS_SERVICE_GCS, ANALYSIS_DATE

def aggregate_trip_speeds_to_route(
    analysis_date: str,
    route_cols: list
) -> pd.DataFrame:
    """
    Start with trip speeds and aggregate to route-level.
    """
    TRIP_SPEED_FILE = GTFS_DATA_DICT.rt_stop_times.trip_speeds_single_summary
    MAX_SPEED = GTFS_DATA_DICT.rt_stop_times.max_speed
    
    speeds = pd.read_parquet(
        f"{SEGMENT_GCS}{TRIP_SPEED_FILE}_{analysis_date}.parquet",
        columns = route_cols + ["trip_instance_key", "speed_mph"],
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    )
    
    speeds_by_route = (speeds.groupby(route_cols, 
                                      observed=True, group_keys=False)
                       .agg({
                           "speed_mph": "mean",
                           "trip_instance_key": "count"
                       }).reset_index()
                       .rename(columns = {"trip_instance_key": "n_trips"})
                      )
    
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
    
    final = final.assign(
        rt_sched_category = final._merge.map(
            gtfs_schedule_wrangling.sched_rt_category_dict)
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