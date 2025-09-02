from path_example_vars import GCS_PATH

import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?

import pandas as pd
import numpy as np
import geopandas as gpd
import json

import shared_utils
import warnings
from path_example_vars import GCS_PATH

from shapely.ops import split, substring, LineString
from calitp_data_analysis import geography_utils

CONVEYAL_GCS_PATH = 'gs://calitp-analytics-data/data-analyses/conveyal_update/'

def read_conveyal_path_df(path):

    array_cols = ['routes', 'boardStops', 'alightStops',
           'rideTimes', 'waitTimes', 'feedIds']

    def unpack_conveyal_path_df(df, array_cols = array_cols):

        for col in array_cols:
            df.loc[:,col] = df[col].map(lambda x: x.split('|'))
        return df

    df = pd.read_csv(path)
    df['total_iterations'] = df.query('origin == 0 & destination == 0').nIterations.iloc[0]
    df = df.query('origin == 0 & destination == 1').drop(columns=['group'])
    df.reset_index(drop=True, inplace=True)
    df.index.rename('trip_group_id', inplace=True)
    #  won't be in Conveyal order but maps will be less confusing
    #  can hash other cols if need to match Conveyal trip-level unique id
    df.reset_index(drop=False, inplace=True)
    df = unpack_conveyal_path_df(df)
    return df

def add_warehouse_identifiers(conveyal_df):
    
    warehouse_conveyal_joined = pd.read_parquet(f'{CONVEYAL_GCS_PATH}warehouse_conveyal_simple_2023-10-18')
    analysis_date = warehouse_conveyal_joined.date.iloc[0].date()

    # all example feeds present?
    unique_feeds = conveyal_df.feedIds.explode().unique()
    assert np.isin(unique_feeds, warehouse_conveyal_joined.feedId).all()
    warehouse_conveyal_joined = warehouse_conveyal_joined[['feedId', 'feed_key', 'gtfs_dataset_name',
                                                           'base64_url', 'date']]
    as_dict = warehouse_conveyal_joined.set_index('feedId').to_dict()

    conveyal_df['feed_keys'] = conveyal_df.feedIds.apply(lambda x: [as_dict['feed_key'][item] for item in x])
    conveyal_df['gtfs_dataset_names'] = conveyal_df.feedIds.apply(lambda x: [as_dict['gtfs_dataset_name'][item] for item in x])
    conveyal_df['date'] = analysis_date
    
    return conveyal_df

def get_warehouse_data(path_df):
    '''
    get relevant data from warehouse for all trips in Conveyal path output
    '''
    analysis_date = path_df.date.iloc[0]
    all_feed_keys = list(path_df.feed_keys.explode().unique())
    all_route_ids = list(path_df.routes.explode().unique())
    all_stops = list(path_df.boardStops.explode().unique()) + list(path_df.alightStops.explode().unique())

    warehouse_data = {}
    warehouse_data['shapes'] = shared_utils.gtfs_utils_v2.get_shapes(selected_date=analysis_date, operator_feeds=all_feed_keys,
                                                      shape_cols = ['feed_key', 'shape_id'])
    warehouse_data['shapes'] = warehouse_data['shapes'].to_crs(geography_utils.CA_NAD83Albers_m)
    warehouse_data['trips'] = shared_utils.gtfs_utils_v2.get_trips(selected_date=analysis_date,
                                                                   operator_feeds=all_feed_keys,
                                                                   trip_cols = ['feed_key', 'name', 'trip_id', 'route_id',
                                                                                'route_short_name', 'route_long_name', 'shape_id',
                                                                                'trip_first_departure_ts']
                                                                  )
    warehouse_data['trips'] = warehouse_data['trips'].query('route_id.isin(@all_route_ids)')
    warehouse_data['st'] = shared_utils.gtfs_utils_v2.get_stop_times(selected_date=analysis_date, operator_feeds=all_feed_keys, trip_df=warehouse_data['trips'],
                                                                    get_df=True)
    warehouse_data['st'] = warehouse_data['st'].query('stop_id.isin(@all_stops)')
    warehouse_data['stops'] = shared_utils.gtfs_utils_v2.get_stops(selected_date=analysis_date, operator_feeds=all_feed_keys, custom_filtering={'stop_id': all_stops})
    warehouse_data['stops'] = warehouse_data['stops'].to_crs(geography_utils.CA_NAD83Albers_m)
    
    return warehouse_data

def shape_segments_from_row(row, warehouse_data, verbose):

    stop_pairs = list(zip(row.boardStops, row.alightStops))
    
    row_shape_segments = []
    for stop_pair in stop_pairs:
        # print(stop_pair)
        first_filter = warehouse_data['st'].query('stop_id.isin(@stop_pair)')
        # display(first_filter)
        good_trips = first_filter.groupby('trip_id')[['stop_id']].count().reset_index().rename(columns={'stop_id':'n'}).query('n > 1')
        #  TODO count frequency using departure sec?
        better_trips = good_trips.query('n == @good_trips.n.max()')
        # display(good_trips)
        assert better_trips.shape[0] > 0
        trip_with_pair = first_filter.query('trip_id == @better_trips.trip_id.iloc[0]').sort_values('stop_sequence')
        trip_with_pair = trip_with_pair[['feed_key', 'trip_id', 'stop_id', 'stop_sequence']]
        trip_with_pair = trip_with_pair.merge(warehouse_data['stops'][['feed_key', 'stop_id', 'geometry']],
                                                      on = ['feed_key', 'stop_id'])
        trip_with_pair = trip_with_pair.merge(warehouse_data['trips'][['feed_key', 'name', 'trip_id', 'shape_id',
                                                                            'route_short_name', 'route_long_name', 'route_id']],
                                                      on = ['feed_key', 'trip_id'])
        paired_shape = warehouse_data['shapes'].query('feed_key == @trip_with_pair.feed_key.iloc[0] & shape_id == @trip_with_pair.shape_id.iloc[0]')
            
        if not trip_with_pair.stop_id.is_unique:
            if verbose:
                print('warning, trip has duplicate stops at a single stop')
            trip_with_pair = trip_with_pair.drop_duplicates(subset=['stop_id'])
        stop0 =  trip_with_pair.query('stop_sequence == @trip_with_pair.stop_sequence.min()').geometry.iloc[0]
        stop1 =  trip_with_pair.query('stop_sequence == @trip_with_pair.stop_sequence.max()').geometry.iloc[0]
        # display(trip_with_pair)
        # print(stop0, stop1)
        if paired_shape.empty:
            if verbose:
                print('warning, trip has no shape')
            trip_with_pair = trip_with_pair.drop_duplicates(subset=['stop_id'])
            paired_segment = LineString([stop0, stop1])
        # stop0_proj = shape_geom.project(stop0)
        # stop1_proj = shape_geom.project(stop1)
        else:
            shape_geom = paired_shape.geometry.iloc[0]
            stops_proj = [shape_geom.project(stop0), shape_geom.project(stop1)] #  be resillient to looping
            paired_segment = substring(shape_geom, min(stops_proj), max(stops_proj))
        
        trip_with_pair['segment_geom'] = paired_segment
        trip_with_pair.set_geometry('segment_geom')
        trip_with_pair = trip_with_pair.rename(columns={'geometry':'stop_geom'})
        # display(stop_pair)
        trip_with_pair = trip_with_pair.drop_duplicates(subset=['shape_id'])
        trip_with_pair['stop_pair'] = [stop_pair]
        trip_with_pair['trip_group_id'] = row.trip_group_id
        trip_with_pair['optimal_pct'] = row.nIterations / row.total_iterations #  more human-friendly name
        # percentige of time in analysis window that this route was optimal
        trip_with_pair['total_time'] = row.totalTime
        trip_with_pair['xfer_count'] = len(stop_pairs) - 1
        trip_with_seg = gpd.GeoDataFrame(trip_with_pair, geometry='segment_geom', crs=geography_utils.CA_NAD83Albers_m)
        row_shape_segments += [trip_with_seg]
        
        spatial_routes = pd.concat(row_shape_segments)
        spatial_routes['route_name'] = spatial_routes.route_short_name.fillna(spatial_routes.route_long_name)
        
    return spatial_routes

def compile_all_spatial_routes(df, warehouse_data, verbose=False):
    spatial_routes = []
    for _ix, row in df.iterrows():
        try:
            spatial_routes += [shape_segments_from_row(row, warehouse_data, verbose)]
        except:
            if verbose:
                print(f'failed for row {row}')
    spatial_routes = pd.concat(spatial_routes)
    return spatial_routes

