import dask.dataframe as dd
import datetime
import numpy as np
import pandas as pd

from shared_utils.rt_utils import MPH_PER_MPS
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
    
    df = df.assign(
        change_meters = df[dist_col] - df.prior_dist,
        change_sec = (df[time_col] - df.prior_time).divide(
                       np.timedelta64(1, 's'))
    )
    
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
    rt_trips: dd.DataFrame,
    analysis_date: str,
    group_cols: list = ["trip_id"]) -> dd.DataFrame:
    """
    Merge RT trips (vehicle positions) to scheduled trips.
    Scheduled trips provides trip_start_time, which is used to find time_of_day.
    """
    keep_cols = [
        "feed_key",
        "direction_id", 
        "route_id", "route_short_name", "route_long_name",
    ] + group_cols
        
    crosswalk = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, 
        keep_trip_cols = keep_cols, 
        get_pandas = True
    ).astype({"direction_id": "Int64"})
    
    time_of_day = sched_rt_utils.get_trip_time_buckets(analysis_date)
    
    df = dd.merge(
        rt_trips,
        crosswalk,
        on = ["gtfs_dataset_key"] + group_cols,
        how = "left",
    ).merge(
        time_of_day,
        on = ["gtfs_dataset_key", "feed_key", "trip_id"],
        how = "left"
    )
    
    return df

if __name__ == "__main__":
    
    start = datetime.datetime.now()
        
    df = pd.read_parquet(
        f"{SEGMENT_GCS}trip_summary/vp_subset_{analysis_date}.parquet",
    )
    
    # in case there are fewer shapes to grab
    shapes_list = df.shape_array_key.unique().tolist()

    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key","geometry"],
        filters = [[("shape_array_key", "in", shapes_list)]],
        get_pandas = True,
        crs = PROJECT_CRS
    )

    linear_ref = wrangle_shapes.linear_reference_vp_against_segment(
        df,
        shapes,
        segment_identifier_cols = ["shape_array_key"]
    ).compute()
    
    time1 = datetime.datetime.now()
    print(f"linear ref: {time1 - start}")
    
    speed = distance_and_seconds_elapsed(
        linear_ref,
        group_cols = ["gtfs_dataset_key", "trip_id"]
    )
    
    speed2 = add_scheduled_trip_columns(
        speed,
        analysis_date,
        group_cols = ["trip_id"]
    )
    
    time2 = datetime.datetime.now()
    print(f"calculate speed: {time2 - time1}")
    
    speed2.to_parquet(
        f"{SEGMENT_GCS}trip_summary/trip_speed_{analysis_date}.parquet"
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")