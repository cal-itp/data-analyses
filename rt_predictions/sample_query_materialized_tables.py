"""
Query materialized table for all trip updates feeds except 511 combined.

For each feed, query only brief sample periods of 0800-0830, 1230-1300, 2300-2330 Pacific.

Query and save intermediate on chunks based on total service hours.
"""

import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(12_000_000_000_000)
os.environ['USE_PYGEOS'] = '0'

import numpy as np
import pandas as pd
from siuba import *

from calitp_data_analysis.sql import query_sql
from calitp_data_analysis.tables import tbls
import datetime as dt

from segment_speed_utils.project_vars import (PREDICTIONS_GCS, 
                                              analysis_date)
analysis_date = dt.datetime.fromisoformat(analysis_date)

sampling_periods = {}
sampling_periods['am'] = [dt.datetime.combine(analysis_date, dt.time(8, 0)),
                     dt.datetime.combine(analysis_date, dt.time(8, 30))]
sampling_periods['mid'] = [dt.datetime.combine(analysis_date, dt.time(12, 30)),
                     dt.datetime.combine(analysis_date, dt.time(13, 0))]
sampling_periods['pm'] = [dt.datetime.combine(analysis_date, dt.time(23, 0)),
                     dt.datetime.combine(analysis_date, dt.time(23, 30))]

analysis_ts = dt.datetime.combine(analysis_date, dt.time(0))

def get_service_levels():
    service_levels = (tbls.mart_gtfs.fct_daily_feed_scheduled_service_summary()
                      >> filter(_.service_date == analysis_date)
                      >> select(_.service_date, _.gtfs_dataset_key, _.ttl_service_hours)
    )
    return service_levels

def get_tu_datasets():
    tu_datasets = (tbls.mart_transit_database.dim_gtfs_datasets()
                  >> filter(_.type == "trip_updates")
                  >> select(_.trip_updates_gtfs_dataset_key == _.key,
                            _.tu_base64_url == _.base64_url)
                  )
    return tu_datasets

def filter_join_datasets_service(tu_datasets, service_levels):
    all_feeds = (tbls.mart_transit_database.dim_provider_gtfs_data()
        >> filter((_.regional_feed_type == None) | (_.regional_feed_type == 'Regional Subfeed'))
        >> filter(_._valid_from <= analysis_ts, _._valid_to > analysis_ts)
        >> filter(_.trip_updates_gtfs_dataset_name != None)
        >> select(_.organization_key, _.organization_name,
                  _.regional_feed_type,
                 _.associated_schedule_gtfs_dataset_key,
                 _.schedule_gtfs_dataset_key, _.schedule_gtfs_dataset_name,
                 _.trip_updates_gtfs_dataset_name, _.trip_updates_gtfs_dataset_key)
        >> inner_join(_, service_levels, on = {'associated_schedule_gtfs_dataset_key': 'gtfs_dataset_key'})
        >> inner_join(_, tu_datasets, on = 'trip_updates_gtfs_dataset_key')
        >> collect()
        >> distinct(_.trip_updates_gtfs_dataset_name, _keep_all=True)
        >> arrange(-_.ttl_service_hours)
    )
    return all_feeds

def chunk_by_svc_hours(df):
    
    chunks = {}
    # https://stackoverflow.com/questions/42763362/dividing-a-pandas-dataframe-into-smaller-chunks-based-of-the-sum-of-one-column
    chunk_target = 10 * 10**3 # ~.75x Metro Bus, which worked
    df = df >> arrange(-_.ttl_service_hours)
    assert (df.ttl_service_hours > chunk_target).value_counts()[True] <= 1, 'only 1st row may exceed chunk target'
    df['running_total'] = df['ttl_service_hours'].cumsum()
    # better to actually round than truncate as in stackoverflow example...
    df['batch'] = np.round((df['running_total'] / chunk_target), 0).astype(int)
    # reset LA Metro Bus chunk to 1 (algo breaks if row exceeds target)
    if df.ttl_service_hours.iloc[0] >= chunk_target:
        df.iloc[0, -1] = 1 # hacky, fix if function ever generalized...
    grouped = df.groupby('batch')
    for group in grouped.groups.keys():
        data = grouped.get_group(group)
        # start at 0, it's pythonic
        chunks[group - 1] =  data     
    return chunks

def get_sample_df(sampling_period, chunk_df):
    
    df = (
        tbls.mart_ad_hoc.fct_stop_time_updates_20230315_to_20230321()
        >> filter(_.service_date == analysis_date,
                  # _.trip_id == '894836',
                  _.arrival_time_pacific >= sampling_period[0],
                  _.arrival_time_pacific < sampling_period[1],
                 _.base64_url.isin(chunk_df.tu_base64_url))
        >> select(_.arrival_time_pacific, _.departure_time_pacific,
                 _.key, _.gtfs_dataset_key, _.base64_url,
                  _._extract_ts, _.trip_update_timestamp, _.trip_id,
                 _.stop_sequence, _.stop_id, _.service_date)
        >> collect()
        >> mutate(no_seq = _.stop_sequence.isna())
)
    return df

def sample_by_chunk_period(sampling_periods, chunks):
        
    for period in sampling_periods.keys():
        for chunk in chunks.keys():
            print(f'period: {period}, chunk: {chunk}')
            print(chunks[chunk].organization_name)
            sample_df = get_sample_df(sampling_periods[period],
                                     chunks[chunk])
            sample_df.to_parquet(
                f"{PREDICTIONS_GCS}st_updates_2023-03-15_{period}_sample/"
                f"chunk_{chunk}.parquet")
            del(sample_df) # memory issues?
    return

if __name__ == "__main__":
    print('already in GCS, uncomment in script to rerun...')
    
    # service_levels = get_service_levels()
    # tu_datasets = get_tu_datasets()
    # all_data_service = filter_join_datasets_service(tu_datasets, service_levels)
    # chunks = chunk_by_svc_hours(all_data_service)
    # sample_by_chunk_period(sampling_periods, chunks)
    