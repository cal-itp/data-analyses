import datetime
import numpy as np
import pandas as pd
import geopandas as gpd

from segment_speed_utils import helpers, vp_transform
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS

from shared_utils import rt_dates
from shared_utils.rt_utils import MPH_PER_MPS
from resample import project_point_onto_shape


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
    intervaled_distance_cutoffs,
):
    
    one_delta_distances_series = []
    one_delta_seconds_series = []
    one_trip_speed_series = []

    for i in range(0, len(intervaled_distance_cutoffs) - 1):
        cut1 = intervaled_distance_cutoffs[i]
        cut2 = intervaled_distance_cutoffs[i+1]
        subset_indices = np.where((one_trip_distance_arr >= cut1) & (one_trip_distance_arr <= cut2))

        subset_distances = one_trip_distance_arr[subset_indices]
        subset_times = one_trip_timestamp_arr[subset_indices]

        # should deltas be returned?
        
        if len(subset_distances > 0):
            delta_distances = subset_distances.max() - subset_distances.min()
            delta_seconds = subset_times.max() - subset_times.min()
            one_speed = delta_distances / delta_seconds* MPH_PER_MPS
            
            one_delta_distances_series.append(delta_distances)
            one_delta_seconds_series.append(delta_seconds)
            one_trip_speed_series.append(one_speed)
            
        else:
            one_delta_distances_series.append(np.nan)
            one_delta_seconds_series.append(np.nan)
            one_trip_speed_series.append(np.nan)
    
    return one_delta_distances_series, one_delta_seconds_series, one_trip_speed_series


def grab_arrays_by_trip2(
    df, 
    distance_type = "",
    intervaled_distance_column_or_meters = ""
):
    
    intervaled_cutoffs = []
    meters_series = []
    seconds_series = []
    speed_series = []
    
    for row in df.itertuples():
        
        one_trip_distance_arr = getattr(row, "interpolated_distances")
        one_trip_timestamp_arr = getattr(row, "resampled_timestamps")
        #should_calculate = np.array(getattr(row, "stop_meters_increasing"))

        
        start_dist = int(np.floor(one_trip_distance_arr).min())
        end_dist = int(np.ceil(one_trip_distance_arr).max())        
        
        if distance_type == "equal_intervals":
            intervaled_distance_cutoffs = np.array(
                range(start_dist, end_dist, intervaled_distance_column_or_meters))

        elif distance_type in ["stop_to_stop", "road_segments"]:
            intervaled_distance_cutoffs = getattr(row, intervaled_distance_column_or_meters)
            #do_not_calculate_indices = np.where(should_calculate == False)[0]
        
        delta_meters_for_trip, delta_seconds_for_trip, speeds_for_trip = get_speeds_every_interval(
            one_trip_distance_arr, 
            one_trip_timestamp_arr,
            intervaled_distance_cutoffs,
        )
        

        #if len(do_not_calculate_indices) > 0:
        #    speeds_for_trip[do_not_calculate_indices] = np.nan
        meters_series.append(delta_meters_for_trip)
        seconds_series.append(delta_seconds_for_trip)
        speed_series.append(speeds_for_trip)
        
    modeled_cols = ["meters_elapsed", "seconds_elapsed", "speed_mph"]
            
    df2 = df.assign(
        meters_elapsed = meters_series,
        seconds_elapsed = seconds_series,
        speed_mph = speed_series    
    )
    
    
    if distance_type == "equal_intervals":
        df2 = df2.assign(
            intervaled_meters = intervaled_cutoffs,
        )
        
        return df2[["trip_instance_key", "intervaled_meters"] + modeled_cols]
    
    elif distance_type == "stop_to_stop":
        return df2[["trip_instance_key", "stop_sequence"] + modeled_cols]
    
    elif distance_type == "road_segments":
        return df2[["trip_instance_key", "linearid", "segment_sequence"] + modeled_cols]
    
    else:
        return


if __name__ == "__main__":
    
    start = datetime.datetime.now()

    analysis_date = rt_dates.DATES["oct2024"]

    INPUT_FILE = GTFS_DATA_DICT.modeled_vp.resampled_vp
    STOP_TIMES_FILE = GTFS_DATA_DICT.modeled_vp.stop_times_on_vp_path
    
    #EXPORT_100METERS = GTFS_DATA_DICT.modeled_vp.speeds_100m
    #EXPORT_250METERS = GTFS_DATA_DICT.modeled_vp.speeds_250m
    EXPORT_STOP_SEGMENTS = GTFS_DATA_DICT.modeled_vp.speeds_by_stop_segments
    '''
    meters_interval = 250
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet"
        #vp_condensed/vp_resampled_{b}_{analysis_date}.parquet,
    )

    results = grab_arrays_by_trip(df, meters_interval)
    results.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_100METERS}_{analysis_date}.parquet"
    )

    end = datetime.datetime.now()
    print(f"{b} speeds every {meters_interval}m: {end - start}")
    '''
    
    #batch0 speeds every 100m: 0:03:00.469936
    #batch1 speeds every 100m: 0:02:50.197037
    #batch0 speeds every 250m: 0:01:32.080767
    #batch1 speeds every 250m: 0:01:38.365538
    #speeds every stop: 
    

    df = pd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet"
    )
    
        
    subset_trips = df.trip_instance_key.unique().tolist()

    stop_time_cutoffs = pd.read_parquet(
        f"{SEGMENT_GCS}{STOP_TIMES_FILE}_{analysis_date}.parquet",
        filters = [[("trip_instance_key", "in", subset_trips)]],
        columns = ["trip_instance_key", "stop_sequence", 
                   "stop_meters", "stop_meters_increasing"]
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
        intervaled_distance_column_or_meters = "stop_meters",
    )
        
    results.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_STOP_SEGMENTS}_{analysis_date}.parquet"
    )

    end = datetime.datetime.now()
    print(f"speeds every stop: {end - start}")