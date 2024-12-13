import pandas as pd
from siuba import *
import numpy as np
import itertools
from segment_speed_utils import helpers, gtfs_schedule_wrangling

from update_vars import (analysis_date, AM_PEAK, PM_PEAK, EXPORT_PATH, GCS_FILE_PATH, PROJECT_CRS,
SEGMENT_BUFFER_METERS, AM_PEAK, PM_PEAK, HQ_TRANSIT_THRESHOLD, MS_TRANSIT_THRESHOLD)

am_peak_hrs = list(range(AM_PEAK[0].hour, AM_PEAK[1].hour))
pm_peak_hrs = list(range(PM_PEAK[0].hour, PM_PEAK[1].hour))
both_peaks_hrs = am_peak_hrs + pm_peak_hrs
peaks_dict = {key: 'am_peak' for key in am_peak_hrs} | {key: 'pm_peak' for key in pm_peak_hrs}

def add_route_dir(
    stop_times: pd.DataFrame,
    analysis_date: str
)-> pd.DataFrame:
    
    trips = helpers.import_scheduled_trips(
    analysis_date,
    columns = ["feed_key", "gtfs_dataset_key", "trip_id",
               "route_id", "direction_id"],
    get_pandas = True
    )

    stop_times = stop_times.merge(
        trips,
        on = ["feed_key", "trip_id"]
    )

    stop_times.direction_id = stop_times.direction_id.fillna(0).astype(int).astype(str)
    stop_times['route_dir'] = stop_times[['route_id', 'direction_id']].agg('_'.join, axis=1)
    return stop_times

def prep_stop_times(
    stop_times: pd.DataFrame,
    am_peak: tuple = AM_PEAK,
    pm_peak: tuple = PM_PEAK
) -> pd.DataFrame:
    """
    Add fixed peak period information to stop_times for next calculations.
    """

    stop_times = stop_times.assign(
            departure_hour = pd.to_datetime(
                stop_times.departure_sec, unit="s").dt.hour
        )

    stop_times = stop_times[stop_times['arrival_hour'].isin(both_peaks_hrs)]
    stop_times['peak'] = stop_times['arrival_hour'].map(peaks_dict)
    
    return stop_times

def stop_times_aggregation_max_by_stop(
    stop_times: pd.DataFrame, 
    analysis_date: str,
    single_route_dir: bool = False,
) -> pd.DataFrame:
    """
    Take the stop_times table 
    and group by stop_id-departure hour
    and count how many trips occur.
    """
    
    stop_cols = ["schedule_gtfs_dataset_key", "stop_id"]
    trips_per_hour_cols = ["peak"]
    
    if single_route_dir:
        trips_per_hour_cols += ["route_id", "direction_id"]

    # Aggregate how many trips are made at that stop by departure hour
    trips_per_peak_period = gtfs_schedule_wrangling.stop_arrivals_per_stop(
        stop_times,
        group_cols = stop_cols + trips_per_hour_cols,
        count_col = "trip_id",
        route_dir_array = True
    ).rename(columns = {"n_arrivals": "n_trips"})
    
    am_trips = (trips_per_peak_period[trips_per_peak_period.peak == 'am_peak']
                .rename(columns = {"n_trips": "am_max_trips"})
                .drop(columns=["peak"])
               )
    pm_trips = (trips_per_peak_period[trips_per_peak_period.peak == 'pm_peak']
                .rename(columns = {"n_trips": "pm_max_trips"})
                .drop(columns=["peak", "route_dir"])
               )
    if single_route_dir:
        am_trips = am_trips.drop(columns=['route_id', 'direction_id'])
        pm_trips = pm_trips.drop(columns=['route_id', 'direction_id'])
    max_trips_by_stop = pd.merge(
        am_trips, 
        pm_trips,
        on = stop_cols,
        how = "left"
    )
    #  divide by length of peak to get trips/hr, keep n_trips a raw sum
    max_trips_by_stop = max_trips_by_stop.assign(
        am_max_trips_hr = (max_trips_by_stop.am_max_trips.fillna(0) / len(am_peak_hrs)).astype(int),
        pm_max_trips_hr = (max_trips_by_stop.pm_max_trips.fillna(0) / len(pm_peak_hrs)).astype(int),
        n_trips = (max_trips_by_stop.am_max_trips.fillna(0) + 
                   max_trips_by_stop.pm_max_trips.fillna(0)),
        route_dir_count = max_trips_by_stop.route_dir.map(lambda x: x.size)
    )
        
    return max_trips_by_stop

if __name__ == "__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    # (1) Aggregate stop times - by stop_id, find max trips in AM/PM peak
    # takes 1 min
    st_prepped = helpers.import_scheduled_stop_times(
        analysis_date,
        get_pandas = True,
    ).pipe(prep_stop_times)
    
    max_arrivals_by_stop_single = st_prepped.pipe(stop_times_aggregation_max_by_stop, analysis_date, single_route_dir=True)
    max_arrivals_by_stop_multi = st_prepped.pipe(stop_times_aggregation_max_by_stop, analysis_date, single_route_dir=False)