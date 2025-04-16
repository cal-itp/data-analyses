import datetime
import numpy as np
import pandas as pd
import geopandas as gpd

from segment_speed_utils import helpers, vp_transform
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS

from shared_utils import rt_dates
from shared_utils.rt_utils import MPH_PER_MPS
from project_condense_resample import project_point_onto_shape

analysis_date = rt_dates.DATES["oct2024"]

def grab_arrays_by_trip(df, meters_interval: int):
    
    intervaled_cutoffs = []
    speed_series = []
    
    for row in df.itertuples():
        
        one_trip_distance_arr = getattr(row, "interpolated_distances")
        one_trip_timestamp_arr = getattr(row, "resampled_timestamps")
        
        start_dist = int(np.floor(one_trip_distance_arr).min())
        end_dist = int(np.ceil(one_trip_distance_arr).max())

        intervaled_distance_cutoffs = np.array(range(start_dist, end_dist, meters_interval))
        
        speeds_for_trip = get_speeds_every_interval(
            one_trip_distance_arr, 
            one_trip_timestamp_arr,
            intervaled_distance_cutoffs
        )
        
        intervaled_cutoffs.append(intervaled_distance_cutoffs)
        speed_series.append(speeds_for_trip)
    
    df2 = df.assign(
        intervaled_meters = intervaled_cutoffs,
        speeds = speed_series
    )[["trip_instance_key", "intervaled_meters", "speeds"]]
    
    return df2
    
    
def get_speeds_every_interval(
    one_trip_distance_arr, 
    one_trip_timestamp_arr,
    intervaled_distance_cutoffs
):
    
    one_trip_speed_series = []

    for i in range(0, len(intervaled_distance_cutoffs) - 1):
        cut1 = intervaled_distance_cutoffs[i]
        cut2 = intervaled_distance_cutoffs[i+1]
        subset_indices = np.where((one_trip_distance_arr >= cut1) & (one_trip_distance_arr <= cut2))

        subset_distances = one_trip_distance_arr[subset_indices]
        subset_times = one_trip_timestamp_arr[subset_indices]

        # should deltas be returned?
        if len(subset_distances > 0):
            one_speed = (
                (subset_distances.max() - subset_distances.min()) / 
                (subset_times.max() - subset_times.min()) 
                * MPH_PER_MPS
            )

            one_trip_speed_series.append(one_speed)
        else:
            one_trip_speed_series.append(np.nan)
    return one_trip_speed_series


def grab_arrays_by_trip2(
    df, 
    distance_type = "",
    intervaled_distance_column_or_meters = ""
):
    
    intervaled_cutoffs = []
    speed_series = []
    
    for row in df.itertuples():
        
        one_trip_distance_arr = getattr(row, "interpolated_distances")
        one_trip_timestamp_arr = getattr(row, "resampled_timestamps")
        
        start_dist = int(np.floor(one_trip_distance_arr).min())
        end_dist = int(np.ceil(one_trip_distance_arr).max())        
        
        if distance_type == "equal_intervals":
            intervaled_distance_cutoffs = np.array(
                range(start_dist, end_dist, intervaled_distance_column_or_meters))

        elif distance_type == "stop_to_stop":
            intervaled_distance_cutoffs = getattr(row, intervaled_distance_column_or_meters)
        
        speeds_for_trip = get_speeds_every_interval(
            one_trip_distance_arr, 
            one_trip_timestamp_arr,
            intervaled_distance_cutoffs
        )
        speed_series.append(speeds_for_trip)

        
        if distance_type == "equal_intervals":
            intervaled_cutoffs.append(intervaled_distance_cutoffs)
            keep_cols = ["intervaled_meters", "speeds"]
        elif distance_type == "stop_to_stop":
            keep_cols = ["speeds", "stop_sequence"]
    
    if distance_type == "equal_intervals":
        df2 = df.assign(
            intervaled_meters = intervaled_cutoffs,
            speeds = speed_series
        )
    elif distance_type == "stop_to_stop":
        df2 = df.assign(
            speeds = speed_series
        )
    
    return df2[["trip_instance_key"] + keep_cols]

    


if __name__ == "__main__":
    '''
    for b in ["batch0", "batch1"]:
        start = datetime.datetime.now()

        meters_interval = 250
        df = pd.read_parquet(
            f"{SEGMENT_GCS}vp_condensed/vp_resampled_{b}_{analysis_date}.parquet",
        )

        results = grab_arrays_by_trip(df, meters_interval)
        results.to_parquet(
            f"{SEGMENT_GCS}rough_speeds_{meters_interval}m_{b}_{analysis_date}.parquet"
        )

        end = datetime.datetime.now()
        print(f"{b} speeds every {meters_interval}m: {end - start}")
    
    
    #batch0 speeds every 100m: 0:03:00.469936
    #batch1 speeds every 100m: 0:02:50.197037
    #batch0 speeds every 250m: 0:01:32.080767
    #batch1 speeds every 250m: 0:01:38.365538
    #batch0 speeds every stop: 0:01:05.459700    
    #batch1 speeds every stop: 0:00:46.450538    
    '''

    for b in ["batch0", "batch1"]:
        start = datetime.datetime.now()

        df = pd.read_parquet(
            f"{SEGMENT_GCS}vp_condensed/vp_resampled_{b}_{analysis_date}.parquet",
        )
        
        subset_trips = df.trip_instance_key.unique().tolist()

        stop_time_cutoffs = pd.read_parquet(
            f"{SEGMENT_GCS}stop_times_projected_{analysis_date}.parquet",
            filters = [[("trip_instance_key", "in", subset_trips)]],
            columns = ["trip_instance_key", "stop_sequence", "stop_meters"]
        )
        
        gdf = pd.merge(
            df,
            stop_time_cutoffs,
            on = "trip_instance_key",
            how = "inner"
        )

        results = grab_arrays_by_trip2(
            gdf,
            distance_type = "stop_to_stop",
            intervaled_distance_column_or_meters = "stop_meters"
        )
        
        results.to_parquet(
            f"{SEGMENT_GCS}rough_speeds_stop_to_stop_{b}_{analysis_date}.parquet"
        )

        end = datetime.datetime.now()
        print(f"{b} speeds every stop: {end - start}")