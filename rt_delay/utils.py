from calitp.tables import tbl
from calitp import query_sql

import shared_utils
from shared_utils.geography_utils import CA_NAD83Albers
from siuba import *

import gcsfs
fs = gcsfs.GCSFileSystem()
import shapely
import datetime as dt
import os
import time
from zoneinfo import ZoneInfo
import pandas as pd
import geopandas as gpd
import warnings

import branca

from numba import jit
import numpy as np

## set system time
os.environ['TZ'] = 'America/Los_Angeles'
time.tzset()

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/rt_delay"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

MPH_PER_MPS = 2.237 ## use to convert meters/second to miles/hour

ZERO_THIRTY_COLORSCALE = branca.colormap.step.RdYlGn_10.scale(vmin=0, vmax=30)
ZERO_THIRTY_COLORSCALE.caption = "Speed (miles per hour)"

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
        return corrected.strip(), extra_day
    else:
        return gtfs_timestring.strip(), extra_day

def gtfs_time_to_dt(df):
    date = df.service_date
    timestring, extra_day = fix_arrival_time(df.arrival_time)
    df['arrival_dt'] = dt.datetime.combine(date + dt.timedelta(days=extra_day),
                        dt.datetime.strptime(timestring, '%H:%M:%S').time())
    return df

def check_cached(filename):
    path = f'{GCS_FILE_PATH}cached_views/{filename}'
    if fs.exists(path):
        return path
    else:
        return None

def get_vehicle_positions(itp_id, analysis_date):
    ''' 
    itp_id: an itp_id (string or integer)
    analysis_date: datetime.date
    
    Interim function for getting complete vehicle positions data for a single operator on a single date of interest.
    To be replaced as RT views are implemented...
    
    Currently drops positions for day after analysis date after 2AM, temporary fix to balance capturing trips crossing
    midnight with avoiding duplicates...
    '''

    
    next_date = analysis_date + dt.timedelta(days = 1)
    date_str = analysis_date.strftime('%Y-%m-%d')
    next_str = next_date.strftime('%Y-%m-%d')
    where = ''
    filename = f'vp_{itp_id}_{date_str}.parquet'
    path = check_cached(filename)
    if path:
        print('found parquet')
        return pd.read_parquet(path)
    else:
        url_numbers = query_sql(f"""SELECT DISTINCT calitp_url_number
    FROM `cal-itp-data-infra.gtfs_rt.vehicle_positions`
    WHERE calitp_itp_id = {itp_id}
    """).calitp_url_number


        ##utc, must bracket 2 days for 1 day pacific...
        for url_number in url_numbers:
            where += f"""OR _FILE_NAME='gs://gtfs-data/rt-processed/vehicle_positions/vp_{date_str}_{itp_id}_{url_number}.parquet'
    OR _FILE_NAME='gs://gtfs-data/rt-processed/vehicle_positions/vp_{next_str}_{itp_id}_{url_number}.parquet'"""

        df = query_sql(f"""SELECT *
    FROM `cal-itp-data-infra.gtfs_rt.vehicle_positions`
    WHERE {where[3:]}
    ORDER BY header_timestamp""") ## where[3:] removes first OR, creating a valid SQL query

        df = df >> distinct(_.vehicle_trip_id, _.vehicle_timestamp, _keep_all=True)
        df = df >> rename(trip_id = _.vehicle_trip_id)
        df = df.dropna(subset=['vehicle_timestamp'])
        assert not df.empty, f'no vehicle positions data found for {date_str}'
        df.vehicle_timestamp = df.vehicle_timestamp.apply(convert_ts)
        df.header_timestamp = df.header_timestamp.apply(convert_ts)

        # assert df.vehicle_timestamp.min() < dt.datetime.combine(analysis_date, dt.time(0)), 'rt data starts after analysis date'
        # assert dt.datetime.combine(analysis_date, dt.time(hour=23, minute=59)) < df.vehicle_timestamp.max(), 'rt data ends early on analysis date'
        if not df.vehicle_timestamp.min() < dt.datetime.combine(analysis_date, dt.time(0)):
            warnings.warn('rt data starts after analysis date')
        if not dt.datetime.combine(analysis_date, dt.time(hour=23, minute=59)) < df.vehicle_timestamp.max():
            warnings.warn('rt data ends early on analysis date')
        next_day_cutoff = dt.datetime.combine(next_date, dt.time(hour=2))
        df = df >> filter(_.vehicle_timestamp < next_day_cutoff,
                         _.vehicle_timestamp > dt.datetime.combine(analysis_date, dt.time(hour=0))
                         )
        df.to_parquet(f'{GCS_FILE_PATH}cached_views/{filename}')
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
    filename = f'trips_{itp_id}_{date_str}.parquet'
    path = check_cached(filename)
    if path:
        print('found parquet')
        return pd.read_parquet(path)
    elif int(itp_id) != 170:
        trips = (tbl.views.gtfs_schedule_fact_daily_trips()
        >> filter(_.calitp_extracted_at <= date_str, _.calitp_deleted_at > next_str)
        >> filter(_.calitp_itp_id == itp_id)
        >> filter(_.service_date == date_str)
        >> filter(_.is_in_service == True)
        >> filter(_.calitp_extracted_at == _.calitp_extracted_at.max())
        >> select(_.trip_key, _.service_date)
        >> inner_join(_, tbl.views.gtfs_schedule_dim_trips(), on = 'trip_key')
        >> select(_.calitp_itp_id, _.calitp_url_number, _.service_date,
                  _.trip_key, _.trip_id, _.route_id, _.direction_id,
                  _.shape_id, _.calitp_extracted_at, _.calitp_deleted_at)
        >> collect()
        >> distinct(_.trip_id, _keep_all=True)
        )
    else:
        trips = (tbl.views.gtfs_schedule_fact_daily_trips()
        # >> filter(_.calitp_extracted_at <= date_str, _.calitp_deleted_at > next_str)
        >> filter(_.calitp_itp_id == itp_id)
        >> filter(_.service_date == date_str)
        >> filter(_.is_in_service == True)
        # >> filter(_.calitp_extracted_at == _.calitp_extracted_at.max())
        >> select(_.trip_key, _.service_date)
        >> inner_join(_, tbl.views.gtfs_schedule_dim_trips(), on = 'trip_key')
        >> select(_.calitp_itp_id, _.calitp_url_number, _.service_date,
                  _.trip_key, _.trip_id, _.route_id, _.direction_id,
                  _.shape_id, _.calitp_extracted_at, _.calitp_deleted_at)
        >> collect()
        >> distinct(_.trip_id, _keep_all=True)
        )
    trips.to_parquet(f'{GCS_FILE_PATH}cached_views/{filename}')
    return trips

def get_stop_times(itp_id, analysis_date, force_clear = False):
    '''
        itp_id: an itp_id (string or integer)
    analysis_date: datetime.date
    
    Interim function for getting complete stop times data for a single operator on a single date of interest.
    To be replaced as RT views are implemented...
    '''
    next_date = analysis_date + dt.timedelta(days = 1)
    date_str = analysis_date.strftime('%Y-%m-%d')
    next_str = next_date.strftime('%Y-%m-%d')
    min_date, max_date = (date_str, next_str)
    filename = f'st_{itp_id}_{date_str}.parquet'
    path = check_cached(filename)
    if path and not force_clear:
        print('found parquet')
        return pd.read_parquet(path)
    elif int(itp_id) != 170:
        trips_query = (tbl.views.gtfs_schedule_fact_daily_trips()
            >> filter(_.calitp_extracted_at <= min_date, _.calitp_deleted_at > max_date)
            >> filter(_.calitp_itp_id == itp_id)
            >> filter(_.service_date == date_str)
            >> filter(_.calitp_extracted_at == _.calitp_extracted_at.max())
            >> filter(_.is_in_service == True)
            >> select(_.trip_key, _.service_date)
            )
        trips_ix_query = (trips_query
            >> inner_join(_, tbl.views.gtfs_schedule_index_feed_trip_stops(), on = 'trip_key')
            >> select(-_.calitp_url_number, -_.calitp_extracted_at, -_.calitp_deleted_at)
            )
        st_query = (tbl.views.gtfs_schedule_dim_stop_times()
            >> filter(_.calitp_itp_id == itp_id)
            >> select(-_.calitp_url_number)
            )
        st = (trips_ix_query
            >> inner_join(_, st_query, on = 'stop_time_key')
            >> mutate(stop_sequence = _.stop_sequence.astype(int)) ## in SQL!
            >> arrange(_.stop_sequence)
            >> collect()
            )
    else:
        trips_query = (tbl.views.gtfs_schedule_fact_daily_trips()
            # >> filter(_.calitp_extracted_at <= min_date, _.calitp_deleted_at > max_date)
            >> filter(_.calitp_itp_id == itp_id)
            >> filter(_.service_date == date_str)
            # >> filter(_.calitp_extracted_at == _.calitp_extracted_at.max())
            >> filter(_.is_in_service == True)
            >> select(_.trip_key, _.service_date)
            )
        trips_ix_query = (trips_query
            >> inner_join(_, tbl.views.gtfs_schedule_index_feed_trip_stops(), on = 'trip_key')
            >> select(-_.calitp_url_number, -_.calitp_extracted_at, -_.calitp_deleted_at)
            )
        st_query = (tbl.views.gtfs_schedule_dim_stop_times()
            >> filter(_.calitp_itp_id == itp_id)
            >> select(-_.calitp_url_number)
            )
        st = (trips_ix_query
            >> inner_join(_, st_query, on = 'stop_time_key')
            >> mutate(stop_sequence = _.stop_sequence.astype(int)) ## in SQL!
            >> arrange(_.stop_sequence)
            >> collect()
            )
    st.to_parquet(f'{GCS_FILE_PATH}cached_views/{filename}')
    return st

def get_stops(itp_id, analysis_date, force_clear = False):
    '''
    itp_id: an itp_id (string or integer)
    analysis_date: datetime.date
    
    Interim function for getting complete stops data for a single operator on a single date of interest.
    To be replaced as RT views are implemented...
    '''
    next_date = analysis_date + dt.timedelta(days = 1)
    date_str = analysis_date.strftime('%Y-%m-%d')
    next_str = next_date.strftime('%Y-%m-%d')
    min_date, max_date = (date_str, next_str)
    filename = f'stops_{itp_id}_{date_str}.parquet'
    path = check_cached(filename)
    if path and not force_clear:
        print('found parquet')
        return gpd.read_parquet(path)
    elif int(itp_id) != 170:
        trips_query = (tbl.views.gtfs_schedule_fact_daily_trips()
        >> filter(_.calitp_extracted_at <= min_date, _.calitp_deleted_at > max_date)
        >> filter(_.calitp_itp_id == itp_id)
        >> filter(_.service_date == date_str)
        >> filter(_.calitp_extracted_at == _.calitp_extracted_at.max())
        >> filter(_.is_in_service == True)
        >> select(_.trip_key, _.service_date)
        )
        trips_ix_query = (trips_query
        >> inner_join(_, tbl.views.gtfs_schedule_index_feed_trip_stops(), on = 'trip_key')
        >> select(-_.calitp_url_number, -_.calitp_extracted_at, -_.calitp_deleted_at)
        )
        stops = (tbl.views.gtfs_schedule_dim_stops()
         >> filter(_.calitp_itp_id == itp_id)
         >> distinct(_.calitp_itp_id, _.stop_id,
                  _.stop_lat, _.stop_lon, _.stop_name, _.stop_key)
         >> inner_join(_, trips_ix_query >> distinct(_.stop_key), on = 'stop_key')
         >> collect()
         >> distinct(_.stop_id, _keep_all=True) ## should be ok to drop duplicates, but must use stop_id for future joins...
         >> select(-_.stop_key)
        )
    else:
        trips_query = (tbl.views.gtfs_schedule_fact_daily_trips()
        # >> filter(_.calitp_extracted_at <= min_date, _.calitp_deleted_at > max_date)
        >> filter(_.calitp_itp_id == itp_id)
        >> filter(_.service_date == date_str)
        # >> filter(_.calitp_extracted_at == _.calitp_extracted_at.max())
        >> filter(_.is_in_service == True)
        >> select(_.trip_key, _.service_date)
        )
        trips_ix_query = (trips_query
        >> inner_join(_, tbl.views.gtfs_schedule_index_feed_trip_stops(), on = 'trip_key')
        >> select(-_.calitp_url_number, -_.calitp_extracted_at, -_.calitp_deleted_at)
        )
        stops = (tbl.views.gtfs_schedule_dim_stops()
         >> filter(_.calitp_itp_id == itp_id)
         >> distinct(_.calitp_itp_id, _.stop_id,
                  _.stop_lat, _.stop_lon, _.stop_name, _.stop_key)
         >> inner_join(_, trips_ix_query >> distinct(_.stop_key), on = 'stop_key')
         >> collect()
         >> distinct(_.stop_id, _keep_all=True) ## should be ok to drop duplicates, but must use stop_id for future joins...
         >> select(-_.stop_key)
        )

    stops = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat),
                            crs='EPSG:4326').to_crs(shared_utils.geography_utils.CA_NAD83Albers)
    export_path = GCS_FILE_PATH+'cached_views/'
    print(export_path)
    print(filename[:-8])
    shared_utils.utils.geoparquet_gcs_export(stops, export_path, filename[:-8])
    return stops

def get_routelines(itp_id):
    '''currently we only have shapes for latest, this will impede historical rt analysis...'''
    path = check_cached(f'{itp_id}_routelines.parquet')
    if path:
        print('found_parquet')
        return gpd.read_parquet(path)
    else:
        routelines = shared_utils.geography_utils.make_routes_shapefile([itp_id], CA_NAD83Albers)
        export_path = GCS_FILE_PATH+'cached_views/'
        shared_utils.utils.geoparquet_gcs_export(routelines, export_path, f'{itp_id}_routelines')
        return routelines
    
def categorize_time_of_day(dt):
    if dt.hour < 4:
        return('Owl')
    elif dt.hour < 7:
        return('Early AM')
    elif dt.hour < 10:
        return('AM Peak')
    elif dt.hour < 15:
        return('Midday')
    elif dt.hour < 20:
        return('PM Peak')
    else:
        return('Evening')
    
@jit(nopython=True) ##numba gives huge speedup here (~60x)
def time_at_position_numba(desired_position, shape_array, dt_float_array):
    if desired_position < shape_array.max() and desired_position > shape_array.min():\
        return np.interp(desired_position, shape_array, dt_float_array)
    else:
        return None

def try_parallel(geometry):
    try:
        return geometry.parallel_offset(25, 'right')
    except:
        return geometry
    
def arrowize_segment(line_geometry, arrow_distance = 15, buffer_distance = 20):
    ''' Given a linestring segment from a gtfs shape, buffer and clip to show direction of progression
    '''
    try:
        # segment = line_geometry.parallel_offset(25, 'right')
        segment = line_geometry.simplify(tolerance = 5)
        if segment.length < 50: ## return short segments unmodified, for now
            return segment.buffer(buffer_distance)
        arrow_distance = max(arrow_distance, line_geometry.length / 20) ##test this out?
        shift_distance = buffer_distance + 1

        begin_segment = shapely.ops.substring(segment, segment.length - 50, segment.length)
        r_shift = begin_segment.parallel_offset(shift_distance, 'right')
        r_pt = shapely.ops.substring(r_shift, 0 , 0)
        l_shift = begin_segment.parallel_offset(shift_distance, 'left')
        l_pt = shapely.ops.substring(l_shift, l_shift.length, l_shift.length)
        end = shapely.ops.substring(begin_segment,
                                begin_segment.length - arrow_distance,
                                begin_segment.length - arrow_distance)
        poly = shapely.geometry.Polygon((r_pt, end, l_pt)) ## triangle to cut bottom of arrow
        ## ends to the left
        end_segment = shapely.ops.substring(segment, 0, 50)
        end = shapely.ops.substring(end_segment, 0, 0) ## correct
        r_shift = end_segment.parallel_offset(shift_distance, 'right')
        r_pt = shapely.ops.substring(r_shift, r_shift.length, r_shift.length)
        r_pt2 = shapely.ops.substring(r_shift, r_shift.length - arrow_distance, r_shift.length - arrow_distance)
        l_shift = end_segment.parallel_offset(shift_distance, 'left')
        l_pt = shapely.ops.substring(l_shift, 0, 0)
        l_pt2 = shapely.ops.substring(l_shift, arrow_distance, arrow_distance)
        t1 = shapely.geometry.Polygon((l_pt2, end, l_pt)) ## triangles to cut top of arrow
        t2 = shapely.geometry.Polygon((r_pt2, end, r_pt))
        segment_clip_mask = shapely.geometry.MultiPolygon((poly, t1, t2))
        # return segment_clip_mask

        differences = segment.buffer(buffer_distance).difference(segment_clip_mask)
        areas = [x.area for x in differences.geoms]
        for geom in differences.geoms:
            if geom.area == max(areas):
                return geom
    except:
        return line_geometry.simplify(tolerance = 5).buffer(buffer_distance)