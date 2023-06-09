import _threshold_utils as threshold_utils
import altair as alt

import pandas as pd
import geopandas as gpd
from segment_speed_utils.project_vars import analysis_date
from shared_utils import calitp_color_palette as cp

GCS_PATH = "gs://calitp-analytics-data/data-analyses/rt_segment_speeds/"

def open_speeds_stops(date: str)-> pd.DataFrame:
    """
    Open up speeds_stops parquet.
    
    Args:
        date: analysis date
    """
    speed_stops_subset = ['gtfs_dataset_key', '_gtfs_dataset_name', 'shape_array_key',
       'stop_sequence', 'trip_id','speed_mph']
    
    df = pd.read_parquet(f"{GCS_PATH}speeds_stop_segments_{date}")
    
    df = df[speed_stops_subset]
    
    return df

def open_avg_speeds(date:str)-> pd.DataFrame:
    """
    Open up avg_speeds_stop_segments parquet.
    
    Args:
        date: analysis date
    """
    df = gpd.read_parquet(f"{GCS_PATH}avg_speeds_stop_segments_{date}.parquet")
    cols_to_drop = ['geometry','geometry_arrowized','district','district_name']
    df = df.drop(columns = cols_to_drop)
    
    return df

def merge_all_speeds(date:str) -> pd.DataFrame:
    """
    Merge avg_speeds_stop_segments and
    speed_stops parquets, drop duplicates, and
    renumber stop sequences that are out of order. 
    
    Args:
        date: analysis date
    """
    avg_speeds = open_avg_speeds(date)
    speed_stops = open_speeds_stops(date)
    
    merge_cols = ['gtfs_dataset_key','shape_array_key', 'stop_sequence']
    m1 = pd.merge(avg_speeds, speed_stops, on = merge_cols, how = 'inner')
    print(f"{len(m1) - len(m1.drop_duplicates())} duplicate rows will be dropped") 
    
    m1 = m1.drop_duplicates().reset_index(drop = True)
    
    return m1

def find_shapes_with_many_stops(date:str) -> pd.DataFrame:
    """
    Find the routes with total stops 
    in the 95th+ percentile.
    
    Args:
        date: analysis_date
        
    Returns:
        DF with the total stops by shape_array_key,
        gtfs_dataset_key, trip_id and total stops to check
        out routes.
        
        List of unique shape_array_keys.
    """
    df = merge_all_speeds(date)
    
    # Find the total number of stops by operator, shape_array_key (route), and trip_id
    agg = (df.groupby(['gtfs_dataset_key','_gtfs_dataset_name','shape_array_key','trip_id'])
                .agg({'stop_sequence':'nunique'})
                .sort_values(['stop_sequence'], ascending = False)
                .reset_index()
                .rename(columns = {'stop_sequence':'total_stops'})
               )
    
    # Categorize 
    p95 =  agg.total_stops.quantile(0.95).astype(float)
    p99 =  agg.total_stops.quantile(0.99).astype(float)
    
    def stop_categories(row):
        if ((row.total_stops >= p95) and (row.total_stops <= p99)):
               return f"95th {p95}-{p99} stops"
        elif row.total_stops > p99:
               return f"99th {p99}+ stops"
        else:
            return "other"
        
    agg["stop_percentiles"] = agg.apply(lambda x: stop_categories(x), axis=1)
    
    # Keep only the routes in the 95th percentile of stops
    agg2 = agg[agg.stop_percentiles.isin([f"95th {p95}-{p99} stops", f"99th {p99}+ stops"])].reset_index(drop = True)
    
    return agg2, agg2.shape_array_key.unique().tolist()