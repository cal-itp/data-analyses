'''
Testing functions to query v2 warehouse instead
'''

import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?
import sys

from siuba import *
import pandas as pd
import geopandas as gpd
import gcsfs
import datetime as dt
import time
import shapely

from rt_analysis import rt_parser
from rt_analysis import rt_filter_map_plot

import shared_utils
from shared_utils.geography_utils import WGS84, CA_NAD83Albers
from calitp_data_analysis.tables import tbls

import shared_utils.rt_utils as rt_utils

# set system time
os.environ["TZ"] = "America/Los_Angeles"
time.tzset()

fs = gcsfs.GCSFileSystem()
BUCKET_NAME = "calitp-analytics-data"
VP_FILE_PATH = f"gs://{BUCKET_NAME}/data-analyses/rt_segment_speeds/"
V2_SUBFOLDER = 'v2_cached_views/'

trip_cols = ['feed_key', 'trip_key', 'gtfs_dataset_key',
             'activity_date', 'trip_id', 'route_id',
             'route_short_name', 'shape_id', 'direction_id',
             'route_type', 'route_long_name', 'route_desc']

st_cols = ['feed_key', 'trip_id', 'stop_id', 'arrival_time',
       'departure_time', 'timepoint', 'stop_sequence', 'continuous_drop_off',
       'continuous_pickup', 'arrival_sec', 'departure_sec']
# must include _sec for util to work...
stop_cols = ['feed_key', 'stop_id', 'stop_name', 'pt_geom']
# must include pt_geom to return gdf
shape_cols = ['feed_key', 'shape_id']

def get_ix_df(itp_id: int, analysis_date: dt.date):
    '''
    An index table for tracking down a given org's schedule/rt feeds
    returns LazyTbl
    '''
    daily_service = (tbls.mart_gtfs.fct_daily_feed_scheduled_service_summary()
    >> select(_.schedule_gtfs_dataset_key == _.gtfs_dataset_key,
             _.feed_key, _.activity_date)
                )

    org_feeds_datasets = (tbls.mart_transit_database.dim_provider_gtfs_data()
    >> filter(_._is_current, _.reports_site_assessed,
            _.organization_itp_id == itp_id,
             _.vehicle_positions_gtfs_dataset_key != None)
            ## think more about how to start/persist org level identifiers...
            ## could be an attribute, or in any case leave first index table as sql...
    >> inner_join(_, daily_service, by = 'schedule_gtfs_dataset_key')
    >> filter(_.activity_date == analysis_date)
    >> select(_.feed_key, _.schedule_gtfs_dataset_key, _.vehicle_positions_gtfs_dataset_key,
             _.organization_itp_id, _.organization_name, _.activity_date)
    )
    
    return org_feeds_datasets >> collect()

def compose_filename_check(ix_df, table):
    
    activity_date = ix_df.activity_date.iloc[0].date()
    date_str = activity_date.strftime(rt_utils.FULL_DATE_FMT)
    assert activity_date == dt.date(2023, 3, 15), 'hardcoded to 3/15 for now :)'
    filename = f"{table}_{ix_df.organization_itp_id.iloc[0]}_{date_str}.parquet"
    path = rt_utils.check_cached(filename = filename, subfolder = V2_SUBFOLDER)
    
    return filename, path, activity_date

def get_vehicle_positions(ix_df):
    '''
    # https://github.com/cal-itp/data-analyses/blob/main/open_data/download_vehicle_positions.py
    # design these tools to read this, filter to organization, write out...
    # starts with warehouse vehicle locations table
    '''
    
    filename, path, activity_date = compose_filename_check(ix_df, 'vp')
    
    if path:
        print(f"found vp parquet at {path}")
        org_vp = gpd.read_parquet(path)
    else:
        vp_all = gpd.read_parquet(f'{VP_FILE_PATH}vp_2023-03-15.parquet')
        org_vp = vp_all >> filter(_.gtfs_dataset_key.isin(ix_df.vehicle_positions_gtfs_dataset_key))
        org_vp = org_vp >> select(-_.location_timestamp)
        org_vp = org_vp.to_crs(CA_NAD83Albers)
        shared_utils.utils.geoparquet_gcs_export(org_vp, rt_utils.GCS_FILE_PATH+V2_SUBFOLDER, filename)

    return org_vp

def get_trips(ix_df):

    filename, path, activity_date = compose_filename_check(ix_df, 'trips')
    
    if path:
        print(f"found trips parquet at {path}")
        org_trips = pd.read_parquet(path)
    else:
        feed_key_list = list(ix_df.feed_key.unique())  
        org_trips = shared_utils.gtfs_utils_v2.get_trips(activity_date, feed_key_list, trip_cols)
        org_trips.to_parquet(rt_utils.GCS_FILE_PATH+V2_SUBFOLDER+filename)
        
    return org_trips

def get_st(ix_df, trip_df):
    
    filename, path, activity_date = compose_filename_check(ix_df, 'st')
    
    if path:
        print(f"found stop times parquet at {path}")
        org_st = pd.read_parquet(path)
    else:
        feed_key_list = list(ix_df.feed_key.unique())  
        org_st = shared_utils.gtfs_utils_v2.get_stop_times(activity_date, feed_key_list, trip_df = trip_df,
                                                     stop_time_cols = st_cols, get_df = True)
        org_st = org_st >> select(-_.arrival_sec, -_.departure_sec)
        org_st.to_parquet(rt_utils.GCS_FILE_PATH+V2_SUBFOLDER+filename)
        
    return org_st

def get_stops(ix_df):
    
    filename, path, activity_date = compose_filename_check(ix_df, 'stops')
    
    if path:
        print(f"found stops parquet at {path}")
        org_stops = gpd.read_parquet(path)
    else:
        feed_key_list = list(ix_df.feed_key.unique())  
        org_stops = shared_utils.gtfs_utils_v2.get_stops(activity_date, feed_key_list, stop_cols,
                                                     crs = CA_NAD83Albers)
        shared_utils.utils.geoparquet_gcs_export(org_stops, rt_utils.GCS_FILE_PATH+V2_SUBFOLDER, filename)
        
    return org_stops

def get_shapes(ix_df):
    
    filename, path, activity_date = compose_filename_check(ix_df, 'shapes')
    
    if path:
        print(f"found shapes parquet at {path}")
        org_shapes = gpd.read_parquet(path)
    else:
        feed_key_list = list(ix_df.feed_key.unique())  
        org_shapes = shared_utils.gtfs_utils_v2.get_shapes(activity_date, feed_key_list, crs = CA_NAD83Albers, 
                                                          shape_cols = shape_cols)
        org_shapes = org_shapes.dropna(subset=['geometry']) ## invalid geos are nones in new df...
        assert type(org_shapes) == type(gpd.GeoDataFrame()) and not org_shapes.empty, 'routelines must not be empty'
        shared_utils.utils.geoparquet_gcs_export(org_shapes, rt_utils.GCS_FILE_PATH+V2_SUBFOLDER, filename)
        
    return org_shapes
