"""
Add some GTFS schedule derived metrics
by trip (service frequency and stop spacing).
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from shared_utils.rt_utils import METERS_PER_MILE
from update_vars import GTFS_DATA_DICT, SCHED_GCS, RT_SCHED_GCS

def assemble_scheduled_trip_metrics(
    analysis_date: str, 
    dict_inputs: dict
) -> pd.DataFrame:
    """
    Get GTFS schedule trip metrics including time-of-day buckets,
    scheduled service minutes, and median stop spacing.
    """
    STOP_TIMES_FILE = dict_inputs.rt_vs_schedule_tables.stop_times_direction
    
    df = gpd.read_parquet(
        f"{RT_SCHED_GCS}{STOP_TIMES_FILE}_{analysis_date}.parquet"
    )

    trips_to_route = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "route_id", "direction_id"],
        get_pandas = True
    )
    
    time_of_day = (gtfs_schedule_wrangling.get_trip_time_buckets(analysis_date)   
                   [["trip_instance_key", "time_of_day", 
                     "scheduled_service_minutes"]]
              )
    
    trip_cols = ["schedule_gtfs_dataset_key", "trip_instance_key"]
    
    grouped_df = df.groupby(trip_cols, observed=True, group_keys=False)

    # Get median / mean stop meters for the trip
    # Attach time-of-day and route_id and direction_id
    df2 = pd.merge(
        grouped_df.agg({"stop_meters": "median"}).reset_index().rename(
            columns = {"stop_meters": "median_stop_meters"}),
        time_of_day,
        on = "trip_instance_key",
        how = "left"
    ).merge(
        trips_to_route,
        on = "trip_instance_key",
        how = "inner"
    )
    
    return df2
    
    
def schedule_metrics_by_route_direction(
    df: pd.DataFrame,
    analysis_date: str,
    group_cols: list
) -> pd.DataFrame:
    """
    Aggregate trip-level metrics to route-direction, and 
    attach shape geometry for common_shape_id.
    """
    
    service_freq_df = gtfs_schedule_wrangling.aggregate_time_of_day_to_peak_offpeak(
        df, group_cols, long_or_wide = "long")
        
    metrics_df = (df.groupby(group_cols, 
                             observed=True, group_keys=False)
                  .agg({
                      "median_stop_meters": "mean", 
                      # take mean of the median stop spacing for trip
                      # does this make sense?
                      # median is the single boiled down metric at the trip-level
                      "scheduled_service_minutes": "mean",
                  }).reset_index()
                  .rename(columns = {
                      "median_stop_meters": "avg_stop_meters",
                      "scheduled_service_minutes": "avg_scheduled_service_minutes"
                  })
                 )
    
    metrics_df = metrics_df.assign(
        avg_stop_miles = metrics_df.avg_stop_meters.divide(METERS_PER_MILE).round(2)
    ).drop(columns = ["avg_stop_meters"])
    
    round_me = ["avg_stop_miles", "avg_scheduled_service_minutes"]
    metrics_df[round_me] = metrics_df[round_me].round(2)

    common_shape = gtfs_schedule_wrangling.most_common_shape_by_route_direction(
        analysis_date
    ).pipe(helpers.remove_shapes_outside_ca)

    df = pd.merge(
        common_shape,
        metrics_df,
        on = group_cols,
        how = "inner"
    ).merge(
        service_freq_df,
        on = group_cols,
        how = "inner"
    )
    
    return df
    
    
if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    TRIP_EXPORT = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_trip_metrics
    ROUTE_DIR_EXPORT = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_route_direction_metrics
    ROUTE_TYPOLOGIES = GTFS_DATA_DICT.schedule_tables.route_typologies
    
    for date in analysis_date_list:
        trip_metrics = assemble_scheduled_trip_metrics(date, GTFS_DATA_DICT)
                
        trip_metrics.to_parquet(
            f"{RT_SCHED_GCS}{TRIP_EXPORT}_{date}.parquet")

        route_cols = [
            "schedule_gtfs_dataset_key", 
            "route_id", 
            "direction_id"
        ]
        
        route_dir_metrics = schedule_metrics_by_route_direction(
            trip_metrics, date, route_cols)
        
        # Right now, this is long, and a route-direction can have 
        # multiple typologies
        route_typologies = pd.read_parquet(
            f"{SCHED_GCS}{ROUTE_TYPOLOGIES}_{date}.parquet",
            columns = route_cols + ["freq_category", "typology", "pct_typology"]
        )
        
        # Select based on plurality, largest pct_typology kept
        # In the future, we can add additional ones
        route_typologies_plurality = route_typologies.sort_values(
            route_cols + ["pct_typology"],
            ascending = [True for i in route_cols] + [False]
        ).drop_duplicates(subset=route_cols).reset_index(drop=True)
        
        route_dir_metrics2 = pd.merge(
            route_dir_metrics,
            route_typologies_plurality,
            on = route_cols,
            how = "left"
        )
            
        utils.geoparquet_gcs_export(
            route_dir_metrics2,
            RT_SCHED_GCS,
            f"{ROUTE_DIR_EXPORT}_{date}"
        )
