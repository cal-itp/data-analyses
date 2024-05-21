from path_example_vars import GCS_PATH

import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?

import pandas as pd
import numpy as np
import geopandas as gpd
from siuba import *
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
    df.index.rename('trip_group_id', inplace=True)
    df.reset_index(inplace=True)
    df['total_iterations'] = (df >> filter(_.origin == 0, _.destination == 0)).nIterations.iloc[0]
    df = (df >> filter(_.origin == 0, _.destination == 1)
             >> select(-_.group)
         )
    df = unpack_conveyal_path_df(df)
    return df

def add_warehouse_identifiers(conveyal_df):
    
    warehouse_conveyal_joined = pd.read_parquet(f'{CONVEYAL_GCS_PATH}warehouse_conveyal_simple_2023-10-18')
    analysis_date = warehouse_conveyal_joined.date.iloc[0].date()

    # all example feeds present?
    unique_feeds = conveyal_df.feedIds.explode().unique()
    assert np.isin(unique_feeds, warehouse_conveyal_joined.feedId).all()

    warehouse_conveyal_joined = warehouse_conveyal_joined >> select(_.feedId, _.feed_key, _.gtfs_dataset_name, _.base64_url, _.date)

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
    warehouse_data['shapes'] = warehouse_data['shapes'].to_crs(geography_utils.CA_NAD83Albers)
    warehouse_data['trips'] = shared_utils.gtfs_utils_v2.get_trips(selected_date=analysis_date,
                                                                   operator_feeds=all_feed_keys,
                                                                   trip_cols = ['feed_key', 'name', 'trip_id', 'route_id',
                                                                                'route_short_name', 'route_long_name', 'shape_id',
                                                                                'trip_first_departure_ts']
                                                                  )
    warehouse_data['trips'] = warehouse_data['trips'] >> filter(_.route_id.isin(all_route_ids))
    warehouse_data['st'] = shared_utils.gtfs_utils_v2.get_stop_times(selected_date=analysis_date, operator_feeds=all_feed_keys, trip_df=warehouse_data['trips'])
    warehouse_data['st'] = warehouse_data['st'] >> filter(_.stop_id.isin(all_stops)) >> collect()
    warehouse_data['stops'] = shared_utils.gtfs_utils_v2.get_stops(selected_date=analysis_date, operator_feeds=all_feed_keys, custom_filtering={'stop_id': all_stops})
    warehouse_data['stops'] = warehouse_data['stops'].to_crs(geography_utils.CA_NAD83Albers)
    
    return warehouse_data

def shape_segments_from_row(row, warehouse_data):

    stop_pairs = list(zip(row.boardStops, row.alightStops))
    
    row_shape_segments = []
    for stop_pair in stop_pairs:
        # print(stop_pair)
        first_filter = warehouse_data['st'] >> filter(_.stop_id.isin(stop_pair))
        # display(first_filter)
        good_trips = first_filter >> count(_.trip_id) >> filter(_.n > 1)
        assert good_trips.shape[0] > 0
        trip_with_pair = first_filter >> filter(_.trip_id == good_trips.trip_id.iloc[0]) >> arrange(_.stop_sequence)
        trip_with_pair = trip_with_pair >> select(_.feed_key, _.trip_id, _.stop_id, _.stop_sequence)
        trip_with_pair = trip_with_pair >> inner_join(_, warehouse_data['stops'] >> select(_.feed_key, _.stop_id, _.geometry),
                                                      on = ['feed_key', 'stop_id'])
        trip_with_pair = trip_with_pair >> inner_join(_, warehouse_data['trips'] >> select(_.feed_key, _.name, _.trip_id, _.shape_id,
                                                                                           _.route_short_name, _.route_long_name),
                                                      on = ['feed_key', 'trip_id'])
        paired_shape = warehouse_data['shapes'] >> filter(_.feed_key == trip_with_pair.feed_key.iloc[0], _.shape_id == trip_with_pair.shape_id.iloc[0])
            
        if not trip_with_pair.stop_id.is_unique:
            print('warning, trip has duplicate stops at a single stop')
            trip_with_pair = trip_with_pair >> distinct(_.stop_id, _keep_all=True)
        stop0 =  (trip_with_pair >> filter(_.stop_sequence == _.stop_sequence.min())).geometry.iloc[0]
        stop1 =  (trip_with_pair >> filter(_.stop_sequence == _.stop_sequence.max())).geometry.iloc[0]
        if paired_shape.empty:
            print('warning, trip has no shape')
            trip_with_pair = trip_with_pair >> distinct(_.stop_id, _keep_all=True)
            paired_segment = LineString([stop0, stop1])
        # stop0_proj = shape_geom.project(stop0)
        # stop1_proj = shape_geom.project(stop1)
        else:
            shape_geom = paired_shape.geometry.iloc[0]
            stops_proj = [shape_geom.project(stop0), shape_geom.project(stop1)] #  be resillient to looping
            paired_segment = substring(shape_geom, min(stops_proj), max(stops_proj))
        
        trip_with_pair['segment_geom'] = paired_segment
        trip_with_pair.set_geometry('segment_geom')
        trip_with_pair = trip_with_pair >> rename(stop_geom = _.geometry)
        # display(stop_pair)
        trip_with_pair = trip_with_pair >> distinct(_.shape_id, _keep_all=True)
        trip_with_pair['stop_pair'] = [stop_pair]
        trip_with_pair['trip_group_id'] = row.trip_group_id
        trip_with_pair['availability_pct'] = row.nIterations / row.total_iterations #  more human-friendly name
        trip_with_pair['total_time'] = row.totalTime
        trip_with_pair['xfer_count'] = len(stop_pairs) - 1
        trip_with_seg = gpd.GeoDataFrame(trip_with_pair, geometry='segment_geom', crs=geography_utils.CA_NAD83Albers)
        row_shape_segments += [trip_with_seg]
        
        spatial_routes = pd.concat(row_shape_segments)
        spatial_routes['route_name'] = spatial_routes.route_short_name.fillna(spatial_routes.route_long_name)
        
    return spatial_routes

def compile_all_spatial_routes(df, warehouse_data, verbose=False):
    spatial_routes = []
    for _ix, row in df.iterrows():
        try:
            spatial_routes += [shape_segments_from_row(row, warehouse_data)]
        except:
            if verbose:
                print(f'failed for row {row}')
    spatial_routes = pd.concat(spatial_routes)
    return spatial_routes

