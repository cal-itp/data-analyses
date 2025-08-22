import pandas as pd

shape_cols = ['feed_key', 'shape_array_key']

trip_cols = ['trip_instance_key', 'trip_id', 'trip_short_name',
'base64_url', 'feed_key', 'name',
'regional_feed_type', 'gtfs_dataset_key', 'service_date',
'direction_id', 'block_id', 'route_key',
'route_id', 'route_type', 'route_short_name',
'route_long_name', 'route_desc', 'agency_id',
'network_id', 'shape_array_key', 'shape_id'
]

stop_cols = ['feed_key', 'stop_id', 'stop_name']

stop_time_cols = ['feed_key', 'trip_id', 'stop_id',
                 'stop_sequence', 'arrival_time', 'departure_time',
                 'arrival_sec', 'departure_sec']

def format_stop_times(trip_st_merged_df: pd.DataFrame) -> pd.DataFrame:
    
    route_cols = [col for col in ['manual_route_name', 'route_short_name', 'route_long_name',
                                 'direction_id'] if col in trip_st_merged_df.columns]
    view_cols = ['stop_id', 'stop_name',
             'trip_id', 'stop_sequence', 'arrival_time',
             'departure_time', 'route_id', 'name',
                 'arrival_sec', 'sort_trip_id', 'geometry']
    view_cols = route_cols + [col for col in view_cols if col in trip_st_merged_df.columns]
    return trip_st_merged_df[view_cols].sort_values(route_cols + ['trip_id', 'stop_sequence'])