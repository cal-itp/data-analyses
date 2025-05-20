import pandas as pd
from siuba import *
import numpy as np
import itertools
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from loguru import logger
import sys
import datetime


from update_vars import (analysis_date, AM_PEAK, PM_PEAK, EXPORT_PATH, GCS_FILE_PATH, PROJECT_CRS,
SEGMENT_BUFFER_METERS, AM_PEAK, PM_PEAK, HQ_TRANSIT_THRESHOLD, MS_TRANSIT_THRESHOLD, SHARED_STOP_THRESHOLD,
ROUTE_COLLINEARITY_KEYS_TO_DROP)

am_peak_hrs = list(range(AM_PEAK[0].hour, AM_PEAK[1].hour))
pm_peak_hrs = list(range(PM_PEAK[0].hour, PM_PEAK[1].hour))
both_peaks_hrs = am_peak_hrs + pm_peak_hrs
peaks_dict = {key: 'am_peak' for key in am_peak_hrs} | {key: 'pm_peak' for key in pm_peak_hrs}

def add_route_dir(
    stop_times: pd.DataFrame,
    analysis_date: str
)-> pd.DataFrame:
    """
    add route and direction to stop times,
    also filter to bus and trolleybus only
    """
    trips = helpers.import_scheduled_trips(
    analysis_date,
    columns = ["feed_key", "gtfs_dataset_key", "trip_id",
               "route_id", "direction_id", "route_type"],
    get_pandas = True
    )
    trips = trips[trips['route_type'].isin(['3', '11'])] #  bus only
    

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
    #  don't count the same trip serving the same stop multiple times -- i.e. trips that start and end at a transit center
    #  the second arrival isn't useful, since the trip ends there: https://www.turlocktransit.com/route6.html
    #  ideally we would filter stop_times on pickup_type and drop_off_type, but those aren't always used
    stop_times = stop_times.drop_duplicates(subset=['schedule_gtfs_dataset_key', 'trip_id', 'stop_id'])
    
    return stop_times

def get_explode_multiroute_only(
    single_route_aggregation: pd.DataFrame,
    multi_route_aggregation: pd.DataFrame,
    frequency_thresholds: tuple,
) -> pd.DataFrame:
    '''
    Shrink the problem space for the compute-intensive collinearity screen.
    First, get stops with any chance of qualifying as either a major stop/hq corr for
    multi-route aggregations, and stops that already may qualify as an hq corr for single-route.
    Be more selective for single route, since some stops may meet the lower frequency as single,
    but if they could meet the higher as multi we want to check collinearity for those.
    Then get stops that appear in multi-route qualifiers only, these will go to
    further processing.
    '''
    #  note this is max -- still evaluate stops meeting the lower threshold as single-route in case they meet the higher threshold as multi
    single_qual_max = single_route_aggregation >> filter(_.am_max_trips_hr >= max(frequency_thresholds), _.pm_max_trips_hr >= max(frequency_thresholds))
    multi_qual = multi_route_aggregation >> filter(_.am_max_trips_hr >= min(frequency_thresholds), _.pm_max_trips_hr >= min(frequency_thresholds))
    multi_only = multi_qual >> anti_join(_, single_qual_max, on=['schedule_gtfs_dataset_key', 'stop_id'])
    #  only consider route_dir that run at least hourly when doing multi-route aggregation, should reduce edge cases
    single_hourly = single_route_aggregation >> filter(_.am_max_trips_hr >= 1, _.pm_max_trips_hr >= 1)
    single_hourly = single_hourly.explode('route_dir')[['route_dir', 'schedule_gtfs_dataset_key', 'stop_id']]
    multi_only_explode = (multi_only[['schedule_gtfs_dataset_key', 'stop_id', 'route_dir']].explode('route_dir'))
    multi_only_explode = multi_only_explode.merge(single_hourly, on = ['route_dir', 'schedule_gtfs_dataset_key', 'stop_id'])
    multi_only_explode = multi_only_explode.sort_values(['schedule_gtfs_dataset_key','stop_id', 'route_dir']) #  sorting crucial for next step
    # print(f'{multi_only_explode.stop_id.nunique()} stops may qualify with multi-route aggregation')
    return multi_only_explode

def accumulate_share_count(route_dir_exploded: pd.DataFrame) -> None:
    '''
    For use via pd.DataFrame.groupby.apply
    Accumulate the number of times each route_dir shares stops with
    each other in a dictionary (share_counts)
    Note impure function -- initialize share_counts = {} before calling
    Could be rewritten with iterrows if desired
    '''
    global share_counts
    rt_dir = route_dir_exploded.route_dir.to_numpy()
    schedule_gtfs_dataset_key = route_dir_exploded.schedule_gtfs_dataset_key.iloc[0]
    for route_dir in rt_dir:
        route = route_dir.split('_')[0] #  don't compare opposite dirs of same route, leads to edge cases like AC Transit 45
        other_dirs = [x for x in rt_dir if x != route_dir and x.split('_')[0] != route]
        for other_dir in other_dirs:
            key = schedule_gtfs_dataset_key+'__'+route_dir+'__'+other_dir
            if key in share_counts.keys():
                share_counts[key] += 1
            else:
                share_counts[key] = 1
                
def feed_level_filter(
    gtfs_dataset_key: str,
    multi_only_explode: pd.DataFrame,
    qualify_dict: dict,
    st_prepped: pd.DataFrame,
    frequency_thresholds: tuple
) -> pd.DataFrame:
    '''
    For a single feed, filter potential stop_times to evaluate based on if their route_dir
    appears at all in qualifying route_dir dict, recheck if there's any chance those stops
    could qualify. Further shrinks problem space for check_stop lookup step
    '''

    this_feed_qual = {key.split(gtfs_dataset_key)[1][2:]:qualify_dict[key] for key in qualify_dict.keys() if key.split('__')[0] == gtfs_dataset_key}
    qualify_pairs = [tuple(key.split('__')) for key in this_feed_qual.keys()]
    arr = np.array(qualify_pairs[0])
    for pair in qualify_pairs[1:]: arr = np.append(arr, np.array(pair))
    any_appearance = np.unique(arr)

    #  only need to check stops that qualify as multi-route only
    stops_to_eval = multi_only_explode >> filter(_.schedule_gtfs_dataset_key == gtfs_dataset_key) >> distinct(_.stop_id)
    st_prepped = st_prepped >> filter(_.schedule_gtfs_dataset_key == gtfs_dataset_key,
                                      _.stop_id.isin(stops_to_eval.stop_id),
                                     )
    # print(f'{st_prepped.shape}')
    st_to_eval = st_prepped >> filter(_.route_dir.isin(any_appearance))
    # print(f'{st_to_eval.shape}')
    #  cut down problem space by checking if stops still could qual after filtering for any appearance
    min_rows = min(frequency_thresholds) * len(both_peaks_hrs)
    st_could_qual = (st_to_eval >> group_by(_.stop_id)
     >> mutate(could_qualify = _.shape[0] >= min_rows)
     >> ungroup()
     >> filter(_.could_qualify)
    )
    # print(f'{st_could_qual.shape}')
    return st_could_qual, qualify_pairs

def check_stop(this_stop_route_dirs, qualify_pairs):
    '''
    Check if all possible route_dir combinations at this stop qualify for aggregation.
    If so, return list of those route_dir. If not, try again excluding the least frequent
    route and continue recursively until a subset all qualifies or combinations are
    exhausted.
    '''
    this_stop_route_dirs = list(this_stop_route_dirs)
    if len(this_stop_route_dirs) == 1:
        # print('exhausted!')
        return []
    # print(f'attempting {this_stop_route_dirs}... ', end='')
    stop_route_dir_pairs = list(itertools.combinations(this_stop_route_dirs, 2))
    checks = np.array([True if rt_dir in qualify_pairs else False for rt_dir in stop_route_dir_pairs])
    if checks.all():
        # print(f'matched!')
        return this_stop_route_dirs
    else:
        # print('subsetting...')
        this_stop_route_dirs.pop(-1)
        return check_stop(this_stop_route_dirs, qualify_pairs)
    
def filter_qualifying_stops(one_stop_st: pd.DataFrame, qualify_pairs: list) -> pd.DataFrame:
    """
    Given stop_times for a single stop, and list of route_dir pairs that can be aggregated,
    filter this stop's stop_times to routes that can be aggregated 
    """
    one_stop_st = (one_stop_st >> group_by(_.route_dir)
                >> mutate(route_dir_count = _.shape[0]) >> ungroup()
                >> arrange(-_.route_dir_count)
               )
    this_stop_route_dirs = (one_stop_st >> distinct(_.route_dir, _.route_dir_count)).route_dir.to_numpy() #  preserves sort order
    aggregation_ok_route_dirs = check_stop(this_stop_route_dirs, qualify_pairs)
    return one_stop_st >> filter(_.route_dir.isin(aggregation_ok_route_dirs))

def collinear_filter_feed(
    gtfs_dataset_key: str,
    multi_only_explode: pd.DataFrame,
    qualify_dict: dict,
    st_prepped: pd.DataFrame,
    frequency_thresholds: tuple
) -> pd.DataFrame:
    '''
    Apply collinearity filtering steps to one feed.
    '''
    
    st_could_qual, qualify_pairs = feed_level_filter(gtfs_dataset_key, multi_only_explode, qualify_dict, st_prepped, frequency_thresholds)
    st_qual_filter_1 = st_could_qual.groupby('stop_id',  group_keys=False).apply(filter_qualifying_stops, qualify_pairs=qualify_pairs)
    st_qual_filter_1 = st_qual_filter_1.reset_index(drop=True)
    if st_qual_filter_1.empty: return
    trips_per_peak_qual_1 = stop_times_aggregation_max_by_stop(st_qual_filter_1, analysis_date, single_route_dir=False)
    trips_per_peak_qual_1 = trips_per_peak_qual_1 >> filter(_.am_max_trips_hr >= min(frequency_thresholds), _.pm_max_trips_hr >= min(frequency_thresholds))
    short_routes = trips_per_peak_qual_1.explode('route_dir') >> count(_.route_dir) >> filter(_.n < SHARED_STOP_THRESHOLD)
    # print('short routes, all_short stops:')
    # display(short_routes)
    trips_per_peak_qual_1['all_short'] = trips_per_peak_qual_1.route_dir.map(
        lambda x: np.array([True if y in list(short_routes.route_dir) else False for y in x]).all())
    # display(trips_per_peak_qual_1 >> filter(_.all_short)) #  stops where _every_ shared route has less than SHARED_STOP_THRESHOLD frequent stops (even after aggregation)
    trips_per_peak_qual_2 = trips_per_peak_qual_1 >> filter(-_.all_short) >> select(-_.all_short)
    
    return trips_per_peak_qual_2

def filter_all_prepare_export(
    feeds_to_filter: list,
    multi_only_explode: pd.DataFrame,
    qualify_dict: dict,
    st_prepped: pd.DataFrame,
    max_arrivals_by_stop_single: pd.DataFrame,
    frequency_thresholds: tuple
):
    '''
    Apply collinearity 
    '''
    #  %%time 90 seconds (on default user) is not too bad! 
    all_collinear = pd.DataFrame()
    for gtfs_dataset_key in feeds_to_filter:
        df = collinear_filter_feed(gtfs_dataset_key, multi_only_explode, qualify_dict,
                                   st_prepped, frequency_thresholds)
        all_collinear = pd.concat([df, all_collinear])
    #  use min here in order to ensure we include stops that meet the lower threshold as single route
    single_qual_min = max_arrivals_by_stop_single >> filter(_.am_max_trips_hr >= min((HQ_TRANSIT_THRESHOLD, MS_TRANSIT_THRESHOLD)),
                                     _.pm_max_trips_hr >= min((HQ_TRANSIT_THRESHOLD, MS_TRANSIT_THRESHOLD)))
    single_only_export = single_qual_min >> anti_join(_, all_collinear, on = ['schedule_gtfs_dataset_key', 'stop_id'])
    combined_export = pd.concat([single_only_export, all_collinear])
    combined_export = combined_export.explode('route_dir')
    combined_export['route_id'] = combined_export['route_dir'].str[:-2]
    
    return combined_export

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
    single_route_cols = []
    
    if single_route_dir:
        single_route_cols += ["route_id", "direction_id"]
        trips_per_hour_cols += single_route_cols

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

    max_trips_by_stop = pd.merge(
        am_trips, 
        pm_trips,
        on = stop_cols + single_route_cols,
        how = "left"
    )
    if single_route_dir:
        max_trips_by_stop = max_trips_by_stop.drop(columns=['route_id', 'direction_id'])
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
    ).pipe(add_route_dir, analysis_date).pipe(prep_stop_times)
    
    max_arrivals_by_stop_single = st_prepped.pipe(stop_times_aggregation_max_by_stop, analysis_date, single_route_dir=True)
    max_arrivals_by_stop_multi = st_prepped.pipe(stop_times_aggregation_max_by_stop, analysis_date, single_route_dir=False)
    
    multi_only_explode = get_explode_multiroute_only(max_arrivals_by_stop_single, max_arrivals_by_stop_multi, (HQ_TRANSIT_THRESHOLD, MS_TRANSIT_THRESHOLD))
    share_counts = {}
    multi_only_explode.groupby(['schedule_gtfs_dataset_key', 'stop_id']).apply(accumulate_share_count)
    qualify_dict = {key: share_counts[key] for key in share_counts.keys() if share_counts[key] >= SHARED_STOP_THRESHOLD}
    for key_part in ROUTE_COLLINEARITY_KEY_PARTS_TO_DROP:
        keys_to_drop += [key for key in qualify_dict.keys() if key_part in key]
    if not len(keys_to_drop) == len(ROUTE_COLLINEARITY_KEY_PARTS_TO_DROP):
        raise Exception("matched keys should exactly equal number of key parts in search")
    for key in keys_to_drop: qualify_dict.pop(key)
    feeds_to_filter = np.unique([key.split('__')[0] for key in qualify_dict.keys()])
    
    combined_export = filter_all_prepare_export(feeds_to_filter, multi_only_explode, qualify_dict,
                                st_prepped, max_arrivals_by_stop_single, (HQ_TRANSIT_THRESHOLD, MS_TRANSIT_THRESHOLD))
    combined_export.to_parquet(f"{GCS_FILE_PATH}max_arrivals_by_stop.parquet")
    
    end = datetime.datetime.now()
    logger.info(
        f"B2_create_aggregate_stop_frequencies {analysis_date} "
        f"execution time: {end - start}")