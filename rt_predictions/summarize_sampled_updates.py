import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(12_000_000_000_000)
os.environ['USE_PYGEOS'] = '0'

import numpy as np
import pandas as pd
import pytz
from siuba import *

from calitp_data_analysis.sql import query_sql
from calitp_data_analysis.tables import tbls
import datetime as dt
import sample_query_materialized_tables as smpl

from segment_speed_utils.project_vars import (PREDICTIONS_GCS, 
                                              analysis_date)
analysis_date = dt.datetime.fromisoformat(analysis_date)


service_levels = smpl.get_service_levels()
tu_datasets = smpl.get_tu_datasets()
all_data_service = smpl.filter_join_datasets_service(tu_datasets, service_levels)
chunks = smpl.chunk_by_svc_hours(all_data_service)
sampling_periods = smpl.sampling_periods

pacific = pytz.timezone('US/Pacific')
end_ts = pacific.localize(dt.datetime.combine(analysis_date, dt.time(23, 59)))

time_to_sec = lambda time: time.hour * 60**2 + time.minute * 60
localize_if_provided = lambda ts: pacific.localize(ts) if not pd.isna(ts) else ts

def get_schedule_df(sampling_period, chunk_df):
    
    start_sec = time_to_sec(sampling_period[0].time())
    end_sec = time_to_sec(sampling_period[1].time())
    
    daily_trips = (tbls.mart_gtfs.fct_daily_scheduled_trips()
                   >> filter(_.trip_first_departure_sec > start_sec,
                           _.trip_first_departure_sec < end_sec)
                   >> filter(_.gtfs_dataset_key.isin(
                       chunk_df.associated_schedule_gtfs_dataset_key))
                   >> filter(_.activity_date == analysis_date)
                   >> select(_.name, _.gtfs_dataset_key, _.trip_id,
                             _.activity_date, _.feed_key, _.route_type
                            )
                   >> collect()
                  )
    
    chunk_tu_url_df = chunk_df >> select(_.tu_base64_url, _.associated_schedule_gtfs_dataset_key,
                                        _.organization_name)
    daily_trips =  daily_trips >> inner_join(_, chunk_tu_url_df,
                                     on = {'gtfs_dataset_key': 'associated_schedule_gtfs_dataset_key'})
    return daily_trips >> select(-_.gtfs_dataset_key)
    
def get_period_chunk(sample_period, chunk):
    assert sample_period in ['am', 'mid', 'pm']
    assert chunk in range(7)
    return pd.read_parquet(
        f"{PREDICTIONS_GCS}st_updates_2023-03-15_{sample_period}_sample/"
        f"chunk_{chunk}.parquet")

def join_trips(sched_chunk_df, tu_chunk_df):
    
    sched_for_join = sched_chunk_df >> select(_.trip_id, _.tu_base64_url, _.feed_key,
                                             _.organization_name, _.route_type)
    tu_sched_joined = tu_chunk_df >> inner_join(_, sched_for_join, 
                   on = {'trip_id': 'trip_id', 'base64_url': 'tu_base64_url'})
    return tu_sched_joined

def check_subset_first_stop(tu_sched_joined):
    '''
    check if stop_sequence is complete from trip updates.
    if so, use that. Otherwise query dim_stop_times and 
    fill in (only) missing values
    '''
    if tu_sched_joined.stop_sequence.isna().any():
        print('filling in some stop_sequence from schedule')
        dim_st = (tbls.mart_gtfs.dim_stop_times()
              >> select(_.feed_key, _.trip_id, _.stop_id,
                       _.st_stop_sequence == _.stop_sequence)
              >> filter(_.feed_key.isin(tu_sched_joined.feed_key.unique()))
              >> filter(_.trip_id.isin(tu_sched_joined.trip_id.unique()))
              >> collect()
             ) # do this here...

        joined = tu_sched_joined >> inner_join(_, dim_st,
                                              on = {'feed_key': 'feed_key',
                                                   'trip_id': 'trip_id',
                                                   'stop_id': 'stop_id'})

        # important to use sequence from trip updates if present
        joined['stop_sequence'] = joined['stop_sequence'].fillna(joined['st_stop_sequence'])
    else:
        joined = tu_sched_joined
    
    first_tu_stops = (joined
                  >> group_by(_.trip_id)
                  >> filter(_.stop_sequence == _.stop_sequence.min())
                  >> ungroup()
                 )
    return first_tu_stops

def add_tz_choose_col(first_stop_df):
    
    first_stop_df =  first_stop_df.dropna(
                        subset = ['trip_update_timestamp'])
    first_stop_df = (first_stop_df
                  >> mutate(tu_ts_pacific = _.trip_update_timestamp.apply(
                      lambda x: x.astimezone(pacific)))
                 )
    
    first_stop_df = (first_stop_df
                 >> mutate(arrival_time_pacific = _.arrival_time_pacific.apply(localize_if_provided))
                 >> mutate(departure_time_pacific = _.departure_time_pacific.apply(localize_if_provided))
                ).dropna(subset = ['trip_update_timestamp'])
    
    return first_stop_df

def calculate_advance_time(first_stop_df):
    
    df = first_stop_df
    df[['departure_time_pacific', 'arrival_time_pacific']] = df[['departure_time_pacific', 'arrival_time_pacific']].fillna(end_ts)
    df['min_arr_dep_pacific'] = df[['departure_time_pacific', 'arrival_time_pacific']].values.min(axis=1)
    df = df >> mutate(time_in_advance = _.min_arr_dep_pacific - _.tu_ts_pacific)
    return df

def group_summarize(advance_calculated_df):
    
    df2 = (advance_calculated_df
       >> group_by(_.trip_id, _.organization_name, _.route_type)
       >> mutate(max_advance = _.time_in_advance.max())
       >> mutate(max_advance_min = _.max_advance.apply(lambda x: x.seconds / 60))
       >> mutate(updates_per_min = _.shape[0] / _.max_advance_min)
       # round minutes to 1 decimal to avoid div/0 later
       >> summarize(max_advance_min = np.round(_.max_advance_min.max(), 1),
                   updates_per_min = np.round(_.updates_per_min.max(), 1))
      )
    return df2

def summarize_all_chunks(sampling_periods, chunks):
    
    collection = []
    for period in sampling_periods.keys():
        for chunk in chunks.keys():
            print(f'period: {period}, chunk: {chunk}')
            tu_chunk_df = get_period_chunk(period, chunk)
            sched_chunk_df = get_schedule_df(sampling_periods[period], chunks[chunk])
            
            tu_sched_joined = join_trips(sched_chunk_df, tu_chunk_df)
            first_stop = check_subset_first_stop(tu_sched_joined)
            with_tz = add_tz_choose_col(first_stop)
            advance_calculated = calculate_advance_time(with_tz)
            grouped_summarized = group_summarize(advance_calculated)
            grouped_summarized['sample_period'] = period
            collection += [grouped_summarized]
            
    summarized = pd.concat(collection)
    summarized.to_parquet(
            f"{PREDICTIONS_GCS}st_advance_samples_summarized_2023-03-15.parquet")
    return

if __name__ == "__main__":
    
    summarize_all_chunks(sampling_periods, chunks)