ANALYSIS_DATE = '2025-09-01'

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