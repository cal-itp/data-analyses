import datetime
import numpy as np
import pandas as pd
import geopandas as gpd

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS

from shared_utils import rt_dates
from shared_utils.rt_utils import MPH_PER_MPS

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


if __name__ == "__main__":

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