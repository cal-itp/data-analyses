"""
Aggregate to stop_id-departure_hour to route-direction.

Aggregate across shape_array_key and stop_sequence. 
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from segment_speed_utils import helpers, gtfs_schedule_wrangling
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)

from shared_utils import calitp_color_palette as cp
from shared_utils import rt_utils

STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")

FILE_NAME = STOP_SEG_DICT['stop_delay_diagnostics']

def get_rt_trip_departure_hour(analysis_date: str) -> dd.DataFrame:
    """
    In the future, we would want to get trip departure hour based
    off of scheduled trips, not RT.
    """
    FILE_NAME = STOP_SEG_DICT['stop_delay_diagnostics']
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{FILE_NAME}_{analysis_date}.parquet", 
        columns = ["gtfs_dataset_key", "_gtfs_dataset_name", "trip_id", 
                   "max_time"]
    ).drop_duplicates()
    
    trip_cols = ["gtfs_dataset_key", "trip_id"]
    timestamp_col = "max_time"
    
    trip_df = (df.groupby(trip_cols)
               [timestamp_col].min().dt.hour
               .reset_index()
              ).rename(columns = {timestamp_col: "departure_hour"})
    
    trip_df = trip_df.assign(
        time_of_day = trip_df.apply(
            lambda x: rt_utils.categorize_time_of_day(x.departure_hour), 
            axis=1)
    )
    
    return trip_df


def aggregate_to_stop_hour_attach_geom(
    df: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Aggregate to stop_id-departure_hour. 
    Attach stop's point geometry.
    Aggregate across shape_arra_keys to get it to route-direction,
    and find max(stop_sequence) so we can correctly order the segments/stops.
    """
    '''
    stop_cols = [
        "gtfs_dataset_key", "_gtfs_dataset_name", 
        "stop_id", "feed_key",
        "route_id", "route_type", "route_short_name", 
        "direction_id",
        "departure_hour", 'time_of_day'
    ]

    metrics = (df.groupby(stop_cols)
               .agg({
                   "actual_minus_scheduled_sec": "mean", 
                   "speed_mph": "mean",
                   "stop_sequence": "max",
                   "trip_id": "nunique",
               }).reset_index()
               .rename(columns = {"trip_id": "n_trips"})
    )
    
    metrics = metrics.assign(
        actual_minus_scheduled_min = (metrics.actual_minus_scheduled_sec
                                      .divide(60).round(1)),
    )
    '''
    df = df.assign(
        actual_minus_scheduled_min = (df.actual_minus_scheduled_sec
                                      .divide(60).round(1)
                                     )
    )
    
    stops = helpers.import_scheduled_stops(
        analysis_date, 
        columns = ["feed_key", "stop_id", "stop_name", "geometry"],
    )

    stop_metrics_with_geom = gtfs_schedule_wrangling.attach_stop_geometry(
        df,
        #metrics, 
        stops,
    ).compute()


    stop_metrics_with_geom = gpd.GeoDataFrame(stop_metrics_with_geom)
    
    return stop_metrics_with_geom


if __name__ == "__main__":
    
    trip_start = get_rt_trip_departure_hour(analysis_date)

    df = pd.read_parquet(
        f"{SEGMENT_GCS}{FILE_NAME}_{analysis_date}.parquet", 
        columns = ["gtfs_dataset_key", "_gtfs_dataset_name", "trip_id", 
                   "shape_array_key", "feed_key",
                   "stop_id", "stop_sequence",
                   "actual_minus_scheduled_sec", "speed_mph",
                  ]
    )
    
    # Merge in route_id and route_type
    scheduled_trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["shape_array_key", "direction_id", 
                   "route_id", "route_short_name", "route_type"]
    ).drop_duplicates().compute()

    df2 = dd.merge(
        df,
        trip_start,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    ).merge(
        scheduled_trips,
        on = "shape_array_key",
        how = "inner"
    )

    # Remove erroneous calculations
    df2 = df2[(df2.speed_mph >= 0) & 
              (df2.speed_mph <= 70)].reset_index(drop=True)

    
    gdf = aggregate_to_stop_hour_attach_geom(df2)
    #gdf.to_parquet(f"./data/stop_metrics_by_hour_{analysis_date}.parquet")
    gdf.to_parquet(f"./data/stop_metrics_disaggregated_{analysis_date}.parquet")