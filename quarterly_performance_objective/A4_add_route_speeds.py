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

def attach_organization_identifiers(
    df: gpd.GeoDataFrame, 
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Take routes categorized with its gtfs_dataset_key and attach
    organization_source_record_id.
    speeds by route comes at source_record_id as identifiers.
    """
    unique_operators = df[["gtfs_dataset_key"]].drop_duplicates()
    
    # Get base64_url, uri, organization_source_record_id and organization_name
    crosswalk = schedule_rt_utils.sample_gtfs_dataset_key_to_organization_crosswalk(
        unique_operators,
        analysis_date,
        quartet_data = "schedule",
        dim_gtfs_dataset_cols = [
            "key",
            "base64_url",
        ],
        dim_organization_cols = ["source_record_id", "name"]
    ).rename(columns = {"schedule_gtfs_dataset_key": "gtfs_dataset_key"})
    
    df_with_org = pd.merge(
        df,
        crosswalk,
        on = "gtfs_dataset_key",
        how = "inner"
    )
    
    return df_with_org


def aggregate_route_direction_speeds(
    analysis_date: str,
    route_cols: list
)-> pd.DataFrame:
    """
    Aggregate route-direction-time-of-day speeds up to route-level.
    """
    speeds = pd.read_parquet(
        f"{SEGMENT_GCS}trip_summary/route_speeds_{analysis_date}.parquet",
        columns = ["org_id", "agency", "route_id", 
                   "speed_mph", "time_of_day", "n_trips"]
    ).rename(columns = {
        "org_id": "organization_source_record_id",
        "agency": "organization_name",
    })
    
    speeds = speeds.assign(
        weighted_speeds = speeds.speed_mph * speeds.n_trips
    )
    
    speeds_by_route = (speeds.groupby(route_cols, 
                                      observed=True, group_keys=False)
                       .agg({"weighted_speeds": "sum", 
                             "n_trips": "sum"})
                       .reset_index()
                       .astype({"n_trips": "int"})
                      )
    
    speeds_by_route = speeds_by_route.assign(
        speed_mph = speeds_by_route.weighted_speeds.divide(
            speeds_by_route.n_trips).round(2)
    ).drop(columns = "weighted_speeds")
    
    return speeds_by_route
    
    
def speed_to_delay_estimate(
    df: gpd.GeoDataFrame, 
    target_speeds_dict: dict
) -> gpd.GeoDataFrame:

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
    ).pipe(attach_organization_identifiers, ANALYSIS_DATE)
    
    route_cols = [
        "organization_source_record_id", "organization_name",
        "route_id"
    ]
    
    speeds_by_route = aggregate_route_direction_speeds(
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
    
    TARGET_SPEEDS_DICT = {
        "on_shn": 16,
        "intersects_shn": 16,
        "other": 25
    }

    final = speed_to_delay_estimate(final, TARGET_SPEEDS_DICT)  
    
    utils.geoparquet_gcs_export(
        final, 
        BUS_SERVICE_GCS,
        f"routes_categorized_with_speed_{ANALYSIS_DATE}"
    )
     
    end = datetime.datetime.now()
    logger.info(f"attach speed: {ANALYSIS_DATE}  {end - start}")