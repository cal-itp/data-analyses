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

# set system time
os.environ["TZ"] = "America/Los_Angeles"
time.tzset()

fs = gcsfs.GCSFileSystem()
BUCKET_NAME = "calitp-analytics-data"
VP_FILE_PATH = f"gs://{BUCKET_NAME}/data-analyses/rt_segment_speeds/"

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

def get_ix_query(itp_id: int, analysis_date: dt.date):
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
    
    return org_feeds_datasets

def get_vehicle_positions(ix_query):
    '''
    # https://github.com/cal-itp/data-analyses/blob/main/open_data/download_vehicle_positions.py
    # design these tools to read this, filter to organization, write out...
    # starts with warehouse vehicle locations table
    '''
    
    ix_df = ix_query >> collect()
    assert ix_df.activity_date.iloc[0].date() == dt.date(2023, 3, 15), 'hardcoded to 3/15 for now :)'
    vp_all = gpd.read_parquet(f'{VP_FILE_PATH}vp_2023-03-15.parquet')
    org_vp = vp_all >> filter(_.gtfs_dataset_key.isin(ix_df.vehicle_positions_gtfs_dataset_key))
    org_vp = org_vp >> select(-_.location_timestamp)
    org_vp = org_vp.to_crs(CA_NAD83Albers)
    return org_vp
