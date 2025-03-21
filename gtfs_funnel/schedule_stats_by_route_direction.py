"""
Add some GTFS schedule derived metrics
by trip (service frequency and stop spacing).
"""
import datetime
import geopandas as gpd
import pandas as pd

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from shared_utils.rt_utils import METERS_PER_MILE
from update_vars import GTFS_DATA_DICT, SCHED_GCS, RT_SCHED_GCS

def cardinal_direction_by_trip(
    stop_times: gpd.GeoDataFrame, 
    group_cols: list
):
    """
    Get a cardinal direction (North, South, East, West) 
    on the route-direction grain.
    Use the stop_times_direction file to count the 
    number of stops for each trip. Most common direction is used
    to designate that trip.
    """
    primary_direction = (
        stop_times.groupby(
            group_cols + ["stop_primary_direction"], 
            dropna=False
        )
        .agg({"stop_sequence": "count"})
        .reset_index()
        .rename(columns={"stop_sequence": "total_stops"})
    ).sort_values(
        group_cols + ["total_stops"], 
        ascending = [True for c in group_cols] + [False]
    ).drop_duplicates(
        subset = group_cols
    )[
        group_cols + ["stop_primary_direction"]
    ].reset_index(drop=True)

    return primary_direction


def assemble_scheduled_trip_metrics(
    analysis_date: str, 
    dict_inputs: dict
) -> pd.DataFrame:
    """
    Get GTFS schedule trip metrics including time-of-day buckets,
    scheduled service minutes, and median stop spacing.
    """
    STOP_TIMES_FILE = dict_inputs.rt_vs_schedule_tables.stop_times_direction
    
    # Load files
    df = gpd.read_parquet(
        f"{RT_SCHED_GCS}{STOP_TIMES_FILE}_{analysis_date}.parquet"
    )
    
    trip_cols = ["trip_instance_key"]
        
    trips_to_route = helpers.import_scheduled_trips(
        analysis_date,
        columns = trip_cols + ["route_id", "direction_id"],
        get_pandas = True
    )
    
    time_of_day = (gtfs_schedule_wrangling.get_trip_time_buckets(analysis_date)   
                   [trip_cols + ["time_of_day", 
                     "scheduled_service_minutes"]]
              )
    
    primary_direction_by_trip = cardinal_direction_by_trip(df, trip_cols)
        
    grouped_df = df.groupby(
        ["schedule_gtfs_dataset_key"] + trip_cols, 
        observed=True, group_keys=False
    )
    
    # Get median / mean stop meters for the trip
    # Attach time-of-day and route_id and direction_id
    # Merge using a subset
    median_stop_meters_df= pd.merge(
        grouped_df.agg({"stop_meters": "median"}).reset_index().rename(
            columns = {"stop_meters": "median_stop_meters"}),
        time_of_day,
        on = trip_cols,
        how = "left"
    ).merge(
        trips_to_route,
        on = trip_cols,
        how = "inner"
    ).merge(
        primary_direction_by_trip,
        on = trip_cols,
        how = "inner"
    )

    return median_stop_meters_df
    
    
def schedule_metrics_by_route_direction(
    df: pd.DataFrame,
    analysis_date: str,
    route_direction_cols: list,
) -> pd.DataFrame:
    """
    Aggregate trip-level metrics to route-direction, and
    attach shape geometry for common_shape_id.
    """
    service_freq_df = gtfs_schedule_wrangling.aggregate_time_of_day_to_peak_offpeak(
        df, 
        route_direction_cols, 
        long_or_wide="long"
    )

    metrics_df = (
        df.groupby(route_direction_cols, 
                   observed=True, group_keys=False, dropna=False)
        .agg(
            {
                "median_stop_meters": "mean",
                # take mean of the median stop spacing for trip
                # does this make sense?
                # median is the single boiled down metric at the trip-level
                "scheduled_service_minutes": "mean",
                "stop_primary_direction": lambda x: x.mode().iloc[0]
            }
        )
        .reset_index()
        .rename(
            columns={
                "median_stop_meters": "avg_stop_meters",
                "scheduled_service_minutes": "avg_scheduled_service_minutes",
                "stop_primary_direction": "route_primary_direction"
            }
        )
    )

    metrics_df = metrics_df.assign(
        avg_stop_miles=metrics_df.avg_stop_meters.divide(METERS_PER_MILE).round(2)
    ).drop(columns=["avg_stop_meters"])

    round_me = ["avg_stop_miles", "avg_scheduled_service_minutes"]
    metrics_df[round_me] = metrics_df[round_me].round(2)
    
    common_shape = gtfs_schedule_wrangling.most_common_shape_by_route_direction(
        analysis_date
    ).pipe(helpers.remove_shapes_outside_ca)


    df = pd.merge(
        common_shape, 
        metrics_df, 
        on = route_direction_cols, 
        how = "inner"
    ).merge(
        service_freq_df, 
        on = route_direction_cols, 
        how = "inner"
    )

    return df
    
    
if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    TRIP_EXPORT = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_trip_metrics
    ROUTE_DIR_EXPORT = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_route_direction_metrics
    ROUTE_TYPOLOGIES = GTFS_DATA_DICT.schedule_tables.route_typologies
    
    for date in analysis_date_list:
        start = datetime.datetime.now()
        
        # Find metrics on the trip grain
        trip_metrics = assemble_scheduled_trip_metrics(date, GTFS_DATA_DICT)
             
        trip_metrics.to_parquet(
            f"{RT_SCHED_GCS}{TRIP_EXPORT}_{date}.parquet")
        
        route_dir_cols = [
            "schedule_gtfs_dataset_key", 
            "route_id", 
            "direction_id"
        ]
        
        route_dir_metrics = schedule_metrics_by_route_direction(
            trip_metrics, date, route_dir_cols)
        
        route_typologies = pd.read_parquet(
            f"{SCHED_GCS}{ROUTE_TYPOLOGIES}_{date}.parquet",
            columns = route_dir_cols + [
                "is_coverage", "is_downtown_local", 
                "is_local", "is_rapid", "is_express", "is_rail"]
        )
        
        
        # Merge cardinal direction & typology work
        route_dir_metrics2 = pd.merge(
            route_dir_metrics,
            route_typologies,
            on = route_dir_cols,
            how = "left"
        )
        
        utils.geoparquet_gcs_export(
            route_dir_metrics2,
            RT_SCHED_GCS,
            f"{ROUTE_DIR_EXPORT}_{date}"
        )
        
        end = datetime.datetime.now()
        print(f"schedule stats for {date}: {end - start}")