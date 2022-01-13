from calitp.tables import tbl
from calitp import query_sql

import shared_utils
from siuba import *

import gcsfs
import shapely
import datetime as dt
import os
import time
from zoneinfo import ZoneInfo
import pandas as pd
import geopandas as gpd


import branca

## set system time
os.environ['TZ'] = 'America/Los_Angeles'
time.tzset()

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/rt_delay"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

MPH_PER_MPS = 2.237 ## use to convert meters/second to miles/hour

def convert_ts(ts):    
    pacific_dt = dt.datetime.fromtimestamp(ts)
    return pacific_dt

def reversed_colormap(existing):
    return branca.colormap.LinearColormap(
        colors=list(reversed(existing.colors)),
        vmin=existing.vmin, vmax=existing.vmax
    )

def primary_cardinal_direction(origin, destination):
    distance_east = destination.x - origin.x
    distance_north = destination.y - origin.y
    
    if abs(distance_east) > abs(distance_north):
        if distance_east > 0:
            return('Eastbound')
        else:
            return('Westbound')
    else:
        if distance_north > 0:
            return('Northbound')
        else:
            return('Southbound')
        
def show_full_df(df):
    with pd.option_context('display.max_rows', None):
        return display(df)
    
def fix_arrival_time(gtfs_timestring):
    '''Reformats a GTFS timestamp (which allows the hour to exceed 24 to mark service day continuity)
    to standard 24-hour time.
    '''
    split = gtfs_timestring.split(':')
    hour = int(split[0])
    extra_day = 0
    if hour >= 24:
        extra_day = 1
        split[0] = str(hour - 24)
        corrected = (':').join(split)
        return corrected.strip()
    else:
        return gtfs_timestring.strip(), extra_day

def gtfs_time_to_dt(df):
    date = df.service_date
    timestring, extra_day = fix_arrival_time(df.arrival_time)
    df['arrival_dt'] = dt.datetime.combine(date + dt.timedelta(days=extra_day),
                        dt.datetime.strptime(timestring, '%H:%M:%S').time())
    return df

def get_vehicle_postions(itp_id, analysis_date):
    ''' 
    itp_id: an itp_id (string or integer)
    analysis_date: datetime.date
    
    Interim function for getting complete vehicle positions data for a single operator on a single date of interest.
    To be replaced as RT views are implemented...
    '''
    
    
    url_numbers = query_sql(f"""SELECT DISTINCT calitp_url_number
FROM `cal-itp-data-infra.gtfs_rt.vehicle_positions`
WHERE calitp_itp_id = {itp_id}
""").calitp_url_number
    
    date_str = analysis_date.strftime('%Y-%m-%d')
    next_str = (analysis_date + dt.timedelta(days = 1)).strftime('%Y-%m-%d')
    where = ''
    ##utc, must bracket 2 days for 1 day pacific...
    for url_number in url_numbers:
        where += f"""
OR _FILE_NAME='gs://gtfs-data/rt-processed/vehicle_positions/vp_{date_str}_{itp_id}_{url_number}.parquet'
OR _FILE_NAME='gs://gtfs-data/rt-processed/vehicle_positions/vp_{next_str}_{itp_id}_{url_number}.parquet'"""
    
    df = query_sql(f"""SELECT *
FROM `cal-itp-data-infra.gtfs_rt.vehicle_positions`
WHERE {where[3:]}
ORDER BY header_timestamp""") ## where[3:] removes first OR, creating a valid SQL query
    
    df = df >> distinct(_.vehicle_trip_id, _.vehicle_timestamp, _keep_all=True)
    df = df >> rename(trip_id = _.vehicle_trip_id)
    df.vehicle_timestamp = df.vehicle_timestamp.apply(convert_ts)
    df.header_timestamp = df.header_timestamp.apply(convert_ts)
    
    assert df.vehicle_timestamp.min() < dt.datetime.combine(analysis_date, dt.time(0)), 'rt data starts after analysis date'
    assert dt.datetime.combine(analysis_date, dt.time(hour=23, minute=59)) < df.vehicle_timestamp.max(), 'rt data ends early on analysis date'
    
    return df

def get_trips(itp_id, analysis_date):
    ''' 
    itp_id: an itp_id (string or integer)
    analysis_date: datetime.date
    
    Interim function for getting complete trips data for a single operator on a single date of interest.
    To be replaced as RT views are implemented...
    '''
    
    date_str = analysis_date.strftime('%Y-%m-%d')
    next_str = (analysis_date + dt.timedelta(days = 1)).strftime('%Y-%m-%d')
    
    trips = (tbl.views.gtfs_schedule_fact_daily_trips()
    >> filter(_.calitp_extracted_at <= date_str, _.calitp_deleted_at > next_str)
    >> filter(_.calitp_itp_id == itp_id)
    >> filter(_.service_date == date_str)
    >> filter(_.calitp_extracted_at == _.calitp_extracted_at.max())
    >> filter(_.is_in_service == True)
    >> select(_.trip_key, _.service_date)
    >> inner_join(_, tbl.views.gtfs_schedule_dim_trips(), on = 'trip_key')
    >> select(_.calitp_itp_id, _.calitp_url_number, _.service_date,
              _.trip_key, _.trip_id, _.route_id, _.direction_id,
              _.shape_id, _.calitp_extracted_at, _.calitp_deleted_at)
    >> collect()
    >> distinct(_.trip_id, _keep_all=True)
    )
    
    return trips