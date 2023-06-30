"""
Use triangulated points, 5 sample vp, per trip
and calculate distance and seconds elapsed.
For each trip, take the sum of the change in distances, change in time,
and calculate speed.

Aggregate trip speeds into route-direction averages by time-of-day.
"""
import dask.dataframe as dd
import datetime
import numpy as np
import pandas as pd

from shared_utils.rt_utils import MPH_PER_MPS
from shared_utils import portfolio_utils, schedule_rt_utils
from segment_speed_utils import helpers, sched_rt_utils, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS,
                                              # analysis_date,
                                              PROJECT_CRS)
analysis_date = "2023-05-17"

def distance_and_seconds_elapsed(
    df: pd.DataFrame, 
    group_cols: list
) -> pd.DataFrame:
    """
    If every trip has 3 vp, we want the change in time and distance
    between 1st and 2nd, 2nd and 3rd.
    Then, sum up the change in time and change by trip.
    """
    dist_col = "shape_meters"
    time_col = "location_timestamp_local"
    sort_cols = group_cols + ["vp_idx"]
    
    # Get the change in distance, time for each row
    df = df.assign(
        prior_dist = (df.sort_values(sort_cols)
                      .groupby(group_cols, 
                               observed=True, group_keys=False)
                      [dist_col]
                      .apply(lambda x: x.shift(1))
                     ),
        prior_time = (df.sort_values(sort_cols)
                      .groupby(group_cols, 
                               observed=True, group_keys=False)
                      [time_col]
                      .apply(lambda x: x.shift(1))
                     )   
    )
    
    # distance should be positive, but sometimes it's not, 
    # so use absolute value
    df = df.assign(
        change_meters = abs(df[dist_col] - df.prior_dist),
        change_sec = (df[time_col] - df.prior_time).divide(
                       np.timedelta64(1, 's'))
    )
    
    # For a trip, sum up the total change in distance and time 
    # Easier to calculate the speed this way, than
    # taking a weighted average later
    df2 = (df.groupby(group_cols, 
                     observed=True, group_keys=False)
           .agg({"change_meters": "sum", 
                 "change_sec": "sum"})
           .reset_index()
          )
    
    df2 = df2.assign(
        speed_mph = (df2.change_meters.divide(df2.change_sec) * 
                     MPH_PER_MPS)
    )
    
    return df2


def add_scheduled_trip_columns(
    rt_trips: pd.DataFrame,
    analysis_date: str,
    group_cols: list = ["trip_id"]) -> pd.DataFrame:
    """
    Merge RT trips (vehicle positions) to scheduled trips.
    Add in the needed scheduled trip columns to take 
    route-direction-time_of_day averages.
    """
    keep_cols = [
        "feed_key",
        "direction_id", 
        "route_id", "route_short_name", "route_long_name", "route_desc",
    ] + group_cols
        
    crosswalk = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, 
        keep_trip_cols = keep_cols, 
        get_pandas = True
    ).astype({"direction_id": "Int64"})
    
    time_of_day = sched_rt_utils.get_trip_time_buckets(analysis_date)
    
    # Clean up route name
    crosswalk2 = portfolio_utils.add_route_name(
        crosswalk
    ).drop(columns = ["route_short_name", "route_long_name", "route_desc"])

    df = dd.merge(
        rt_trips,
        crosswalk2,
        on = ["gtfs_dataset_key"] + group_cols,
        how = "left",
    ).merge(
        time_of_day,
        on = ["gtfs_dataset_key", "feed_key", "trip_id"],
        how = "left"
    )
    
    return df


def drop_extremely_low_and_high_speeds(
    df: pd.DataFrame, 
    speed_range: tuple
) -> pd.DataFrame:
    """
    Descriptives show the 5th percentile is around 5 mph, 
    and 95th percentile is around 25 mph.
    
    There are some weird calculations for <3 mph, and even
    some negative values, so let's exclude those...maybe
    the vp is not traveling across the entirety of the shape.
    
    Exclude unusually high speeds, over 70 mph.
    """
    low, high = speed_range
    
    df2 = df[(df.speed_mph >= low) & 
             (df.speed_mph <= high)
            ].reset_index(drop=True)
    
    return df2


def avg_route_speeds_by_time_of_day(
    df: pd.DataFrame,
    group_cols: list,
    speed_range: tuple = (3, 70)
) -> pd.DataFrame:
    """
    Keep trips with average speeds at least LOWER_BOUND_SPEED
    and less than or equal to UPPER_BOUND_SPEED.
    
    Take the average by route-direction-time_of_day.
    Also include averages for scheduled trip service_minutes vs 
    rt trip pproximated-service-minutes
    """
    df2 = drop_extremely_low_and_high_speeds(df, speed_range = (3, 70))
    
    df3 = (df2.groupby(group_cols)
           .agg({
               "speed_mph": "mean",
               "service_minutes": "mean",
               "change_sec": "mean",
               "trip_id": "count"
           }).reset_index()
          )
    
    df3 = df3.assign(
        avg_rt_trip_min = df3.change_sec.divide(60),
    ).rename(columns = {"service_min": "avg_sched_trip_min"})
    
    return df3


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    # Merge in the subset of vp to the shape geometry
    df = pd.read_parquet(
        f"{SEGMENT_GCS}trip_summary/vp_subset_{analysis_date}.parquet",
    )
    
    # in case there are fewer shapes to grab
    shapes_list = df.shape_array_key.unique().tolist()

    # to_crs() takes a long time when os.environ["USE_PYGEOS"] = '0',
    # so keep pygeos on
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key","geometry"],
        filters = [[("shape_array_key", "in", shapes_list)]],
        get_pandas = True,
        crs = PROJECT_CRS
    )
    
    # project the vp geometry onto the shape geometry and get shape_meters
    linear_ref = wrangle_shapes.linear_reference_vp_against_segment(
        df,
        shapes,
        segment_identifier_cols = ["shape_array_key"]
    ).compute()
    
    time1 = datetime.datetime.now()
    print(f"linear ref: {time1 - start}")
    
    # Get trip-level speed
    speed = distance_and_seconds_elapsed(
        linear_ref,
        group_cols = ["gtfs_dataset_key", "trip_id"]
    )
    
    # Attach scheduled trip columns, like route, direction, time_of_day
    speed2 = add_scheduled_trip_columns(
        speed,
        analysis_date,
        group_cols = ["trip_id"]
    )
    
    time2 = datetime.datetime.now()
    print(f"calculate speed: {time2 - time1}")
    
    speed2.to_parquet(
        f"{SEGMENT_GCS}trip_summary/trip_speeds_{analysis_date}.parquet"
    )
    
    # Take the average across route-direction-time_of_day
    avg_speeds = avg_route_speeds_by_time_of_day(
        speed2, 
        group_cols = [
            "gtfs_dataset_key", "time_of_day",
            "route_id", "direction_id",
            "route_name_used"
        ]
    )
    
    # Attach org name and source_record_id
    org_crosswalk = (
        schedule_rt_utils.sample_gtfs_dataset_key_to_organization_crosswalk(
            avg_speeds,
            analysis_date,
            quartet_data = "vehicle_positions",
            dim_gtfs_dataset_cols = ["key", "base64_url"],
            dim_organization_cols = ["source_record_id", 
                                     "name", "caltrans_district"]
        )
    )
    
    avg_speeds_with_org = pd.merge(
        avg_speeds,
        org_crosswalk.rename(columns = {
            "vehicle_positions_gtfs_dataset_key": "gtfs_dataset_key"}),
        on = "gtfs_dataset_key",
        how = "inner"
    )

    avg_speeds_with_org.to_parquet(
        f"{SEGMENT_GCS}trip_summary/route_speeds_{analysis_date}.parquet")
    
    time3 = datetime.datetime.now()
    print(f"route-direction average speeds: {time3 - time2}")
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")