import pandas as pd

def format_stop_times(trip_st_merged_df: pd.DataFrame) -> pd.DataFrame:
    
    route_cols = [col for col in ['manual_route_name', 'route_short_name', 'route_long_name',
                                 'direction_id'] if col in trip_st_merged_df.columns]
    view_cols = ['stop_id', 'stop_name',
             'trip_id', 'stop_sequence', 'arrival_time',
             'departure_time', 'route_id', 'name',
                 'arrival_sec', 'sort_trip_id', 'geometry']
    view_cols = route_cols + [col for col in view_cols if col in trip_st_merged_df.columns]
    return trip_st_merged_df[view_cols].sort_values(route_cols + ['trip_id', 'stop_sequence'])