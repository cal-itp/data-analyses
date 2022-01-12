import shared_utils

import gcsfs
import shapely
import datetime as dt
import pandas as pd

import branca

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/rt_delay"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

MPH_PER_MPS = 2.237 ## use to convert meters/second to miles/hour

def convert_ts(ts):    
    pacific_dt = dt.datetime.fromtimestamp(ts)
    # print(pacific_dt)
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