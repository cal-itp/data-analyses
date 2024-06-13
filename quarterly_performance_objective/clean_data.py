"""
Set up route df for service hours and speeds.
Routes are categorized as on_shn,
parallel/intersecting, or other.
"""
import datetime
import geopandas as gpd
import pandas as pd

from segment_speed_utils import (helpers, gtfs_schedule_wrangling,
                                 parallel_corridors)
from calitp_data_analysis import geography_utils, utils
from update_vars import (
    GTFS_DATA_DICT, SEGMENT_GCS,
    BUS_SERVICE_GCS, ANALYSIS_DATE
)


def aggregate_to_route_service_hours(analysis_date: str) -> pd.DataFrame:
    """
    Aggregate service hours from trips table to route-level.
    """
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "name",
                   "route_key", "trip_instance_key", "service_hours"],
        get_pandas = True
    )
    
    df2 = (
        trips.groupby(["feed_key", "name", "route_key"])
        .agg({
           "trip_instance_key": "nunique",
           "service_hours": "sum"
       }).reset_index()
        .rename(columns = {
            "trip_instance_key": "n_trips",
        })
    )
    
    return df2


def get_route_summary_speed(analysis_date: str) -> pd.DataFrame:
    """
    We have route-dir summary speeds.
    Aggregate here and get route summary speed.
    """
    FILE = GTFS_DATA_DICT.rt_stop_times.route_dir_single_summary
    
    # Import all_day average summary speeds
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{FILE}_{analysis_date}.parquet",
        columns = ["schedule_gtfs_dataset_key", 
                   "route_id", "direction_id", "route_name",
                   "speed_mph"],
        filters = [[("time_period", "==", "all_day")]]
    )
    
    # Aggregate to route from route-dir
    df2 = (df.groupby(["schedule_gtfs_dataset_key", 
                       "route_id", "route_name"])
           .agg({"speed_mph": "mean"})
           .reset_index()
          )
    
    return df2
    

def process_df(analysis_date: str) -> gpd.GeoDataFrame:
    """
    """
    # Get gdf of unique routes tagging them by on_shn/parallel/other
    df = parallel_corridors.routes_by_on_shn_parallel_categories(analysis_date)
    
    # Get df of route service hours
    route_service_hours = aggregate_to_route_service_hours(analysis_date)

    # Get crosswalk linking schedule_gtfs_dataset_key to the organization_name (which we use for portfolio)
    # and caltrans_district
    crosswalk = helpers.import_schedule_gtfs_key_organization_crosswalk(
        analysis_date,
        columns = ["schedule_gtfs_dataset_key", "organization_name", 
                   "caltrans_district"]
    )
    
    # Get route summary speeds
    route_speeds = get_route_summary_speed(analysis_date)

    df2 = pd.merge(
        df,
        route_service_hours,
        on = ["feed_key", "route_key"],
        how = "inner"
    )
    
    df3 = pd.merge(
        df2, 
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner",
        validate = "m:1",
    )

    # Change route_length into miles instead of feet for understandability
    df3 = df3.assign(
        route_length_miles = df3.route_length_feet.divide(
            geography_utils.FEET_PER_MI)
    ).drop(columns = "route_length_feet")
    
    df4 = pd.merge(
        df3,
        route_speeds,
        on = ["schedule_gtfs_dataset_key", "route_id"],
        how = "left",
        validate = "1:1",
    )
    
    round_me = ["service_hours", "route_length_miles", "speed_mph"]
    df4[round_me] = df4[round_me].round(2)
    
    return df4


if __name__ == "__main__":
        
    start = datetime.datetime.now()

    gdf = process_df(ANALYSIS_DATE)

    utils.geoparquet_gcs_export(
        gdf,
        BUS_SERVICE_GCS,
        f"routes_categorized_with_speed_{ANALYSIS_DATE}"
    )

    end = datetime.datetime.now()
    print(f"quarterly route df for {ANALYSIS_DATE}: {end - start}")