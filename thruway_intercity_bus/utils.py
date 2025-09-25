import pandas as pd
import update_vars

def format_stop_times(trip_st_merged_df: pd.DataFrame) -> pd.DataFrame:
    
    route_cols = [col for col in ['manual_route_name', 'route_short_name', 'route_long_name',
                                 'direction_id'] if col in trip_st_merged_df.columns]
    view_cols = ['stop_id', 'stop_name',
             'trip_id', 'stop_sequence', 'arrival_time',
             'departure_time', 'route_id', 'name',
                 'arrival_sec', 'sort_trip_id', 'geometry']
    view_cols = route_cols + [col for col in view_cols if col in trip_st_merged_df.columns]
    return trip_st_merged_df[view_cols].sort_values(route_cols + ['trip_id', 'stop_sequence'])

def read_format_ridership() -> pd.DataFrame:
    
    source_ridership = pd.read_excel(update_vars.RIDERSHIP_PATH)
    source_ridership = source_ridership.assign(od = source_ridership.orig + '->' + source_ridership.dest)
    source_ridership = source_ridership.assign(route_short_name = source_ridership.ca_bus_route.str.replace('Rt', 'Route'))

    rider_to_gtfs_dict = {'Route 1A': 'Route 1', 'Route 1B': 'Route 1', 'Route 1C': 'Route 1c',
                         'Route 20 - B': 'Route 20', 'Route 3R': 'Route 3'}
    strip_zero = lambda route_str: ' '.join([x.lstrip('0') for x in route_str.split(' ')])
    rider_to_gtfs = lambda route_str: rider_to_gtfs_dict[route_str] if route_str in rider_to_gtfs_dict.keys() else route_str

    source_ridership = source_ridership.assign(route_short_name = source_ridership.route_short_name.map(strip_zero).map(rider_to_gtfs))
    
    return source_ridership

def longest_by_route_dir(shape_trip_df):
    #  https://github.com/cal-itp/data-analyses/blob/1abcd5d05163176567f644f6f245366dd9cc53fc/rt_segment_speeds/segment_speed_utils/gtfs_schedule_wrangling.py#L426
    sort_cols = ['route_id', 'direction_id']
    df = (shape_trip_df.query('route_type == "3"')
     .sort_values(sort_cols + ['length_meters'], ascending=[True for i in sort_cols] + [False])
     .drop_duplicates(subset=sort_cols)
     .reset_index(drop=True)
    )
    return df