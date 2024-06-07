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

def find_most_common_dir(
    stop_times_gdf: gpd.GeoDataFrame,
    trips_to_route_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Instead of only having direction_id as 0/1, add back the
    cardinal directions. 
    """
    stop_times_col = [
        "feed_key",
        "stop_id",
        "stop_sequence",
        "schedule_gtfs_dataset_key",
        "trip_instance_key",
        "shape_array_key",
        "stop_name",
        "prior_stop_sequence",
        "subseq_stop_sequence",
        "stop_pair",
        "stop_pair_name",
        "stop_primary_direction",
        "stop_meters",
    ]
    
    # Keep only a subset
    stop_times_gdf2 = stop_times_gdf[stop_times_col]

    # Merge dfs
    merge_cols = ["trip_instance_key", 
                  "schedule_gtfs_dataset_key", 
                  "shape_array_key"]

    df1 = pd.merge(stop_times_gdf2, trips_to_route_df, on=merge_cols, how="inner")
    
    # Fill in missing direction id with 0, per our usual practice.
    df1.direction_id = df1.direction_id.fillna(0)
    
    agg1 = (
        df1.groupby(
            [
                "route_id",
                "schedule_gtfs_dataset_key",
                "direction_id",
                "stop_primary_direction",
            ]
        )
        .agg({"stop_sequence": "count"})
        .reset_index()
        .rename(columns={"stop_sequence": "total_stops"})
    )

    # Sort and drop duplicates so that the
    # largest # of stops by stop_primary_direction is at the top
    agg2 = agg1.sort_values(
        by=["route_id", 
            "schedule_gtfs_dataset_key",
            "direction_id",
            "total_stops"],
        ascending=[True, True, True, False],
    )

    # Drop duplicates so only the top stop_primary_direction is kept.
    agg3 = agg2.drop_duplicates(
    subset=[
        "route_id",
        "schedule_gtfs_dataset_key",
        "direction_id",
    ]).reset_index(drop=True)
    
    agg3 = agg3.drop(columns=["total_stops"])
    return agg3

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
    
    scheduled_col = [
    "route_id",
    "trip_instance_key",
    "gtfs_dataset_key",
    "shape_array_key",
    "direction_id",
    "route_long_name",
    "route_short_name",
    "route_desc",
    "name"
    ]
    
    trips_to_route = helpers.import_scheduled_trips(
        analysis_date,
        columns = scheduled_col,
        get_pandas = True
    )
    
    time_of_day = (gtfs_schedule_wrangling.get_trip_time_buckets(analysis_date)   
                   [["trip_instance_key", "time_of_day", 
                     "scheduled_service_minutes"]]
              )
    
    trip_cols = ["schedule_gtfs_dataset_key", "trip_instance_key"]
    
    grouped_df = df.groupby(trip_cols, observed=True, group_keys=False)
    
    trips_to_route_cols_subset = ["trip_instance_key", "route_id", "direction_id"]
    
    # Get median / mean stop meters for the trip
    # Attach time-of-day and route_id and direction_id
    # Merge using a subset
    median_stop_meters_df= pd.merge(
        grouped_df.agg({"stop_meters": "median"}).reset_index().rename(
            columns = {"stop_meters": "median_stop_meters"}),
        time_of_day,
        on = "trip_instance_key",
        how = "left"
    ).merge(
        trips_to_route[trips_to_route_cols_subset],
        on = "trip_instance_key",
        how = "inner"
    )
    
    median_stop_meters_df.direction_id = median_stop_meters_df.direction_id.fillna(0)
    
    # Get cardinal direction
    cardinal_direction_df = find_most_common_dir(df,trips_to_route)
    
    # Merge everything together
    m1 = pd.merge(
    median_stop_meters_df,
    cardinal_direction_df,
    on=["schedule_gtfs_dataset_key", "route_id", "direction_id"],
    how="inner")
    
    return m1
    
    
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
    
    # AH CHANGE THIS LATER
    for date in analysis_date_list[0:3]:
        start = datetime.datetime.now()
        
        trip_metrics = assemble_scheduled_trip_metrics(date, GTFS_DATA_DICT)
        
        # AH CHANGE THIS LATER
        trip_metrics.to_parquet(
            f"{RT_SCHED_GCS}{TRIP_EXPORT}_{date}_AH_TESTING.parquet")
        
        route_cols = [
            "schedule_gtfs_dataset_key", 
            "route_id", 
            "direction_id"
        ]
        
        route_dir_metrics = schedule_metrics_by_route_direction(
            trip_metrics, date, route_cols)
        
        route_typologies = pd.read_parquet(
            f"{SCHED_GCS}{ROUTE_TYPOLOGIES}_{date}.parquet",
            columns = route_cols + [
                "is_coverage", "is_downtown_local", 
                "is_local", "is_rapid", "is_express", "is_rail"]
        )
        
        route_dir_metrics2 = pd.merge(
            route_dir_metrics,
            route_typologies,
            on = route_cols,
            how = "left"
        )
        
        # AH CHANGE THIS LATER
        utils.geoparquet_gcs_export(
            route_dir_metrics2,
            RT_SCHED_GCS,
            f"{ROUTE_DIR_EXPORT}_{date}_AH_TESTING"
        )
        
        end = datetime.datetime.now()
        print(f"schedule stats for {date}: {end - start}")
