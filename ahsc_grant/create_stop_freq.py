import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)

import branca
import folium
from shared_utils import gtfs_utils_v2

from siuba import *
import pandas as pd
import geopandas as gpd 

import datetime as dt
import time

import seaborn as sns
import matplotlib.pyplot as plt


#Function to fetch feeds, trips, stoptimes and stops_geo data from warehouse v2
def get_feeds_trips_stops_data(selected_agencies, selected_date):
    
    trip_cols = ["name", "gtfs_dataset_key", "feed_key", "trip_id", "route_id", "route_type"]
    stoptimes_cols = ["key", "_gtfs_key", "feed_key", "trip_id", "stop_id"]
    stop_cols = ["feed_key", "stop_id", "geometry", "stop_name", "stop_code", "location_type", "stop_desc"]
    
    feeds = gtfs_utils_v2.schedule_daily_feed_to_gtfs_dataset_name(selected_date=selected_date)
    
    def select_by_agency(df, column, values):
        if isinstance(values, str):
            values = [values]  # Convert single string to list of strings
        
        selected_df_list = []
        for value in values:
            selected_df = df[df[column].str.contains(value, case=False)].copy()
            selected_df_list.append(selected_df)
        
        selected_df_concat = pd.concat(selected_df_list, ignore_index=True)
        return selected_df_concat
    
    feed_data = select_by_agency(feeds, 'name', selected_agencies)
    
    if feed_data.empty:
        raise ValueError(f"No feeds data found for agencies '{selected_agencies}' on {selected_date}.")
    
    feed_key_list = feed_data['feed_key'].tolist()
    
    trips_data_list = []
    stoptimes_data_list = []
    stop_locations_gdf = gpd.GeoDataFrame()

    for feed_key in feed_key_list:
        trips = gtfs_utils_v2.get_trips(selected_date=selected_date, operator_feeds=[feed_key])[trip_cols]
        trips_data_list.append(trips)
        
        stoptimes = gtfs_utils_v2.get_stop_times(selected_date=selected_date, operator_feeds=[feed_key], 
                                                trip_df=trips, get_df=True)[stoptimes_cols]
        stoptimes_data_list.append(stoptimes)
        
        stops_gdf = gtfs_utils_v2.get_stops(selected_date=selected_date, operator_feeds=[feed_key])[stop_cols]
        stop_locations_gdf = pd.concat([stop_locations_gdf, stops_gdf], ignore_index=True)
    
    trips_data = pd.concat(trips_data_list, ignore_index=True)
    stoptimes_data = pd.concat(stoptimes_data_list, ignore_index=True)
    
    return feed_data, trips_data, stoptimes_data, stop_locations_gdf


#Function to analyze data
def analyze_dataset(df):
    # Number of rows and columns
    num_rows, num_cols = df.shape 
    print(f"Number of rows: {num_rows}, Number of columns: {num_cols}")
    print()
    
    # Print column names 
    column_names = df.columns.tolist()
    print(f"Column names: \n{column_names}\n")
    
    # Print data types
    print("Data types:")
    print(df.dtypes)
    print()
          
    # Check for duplicates
    duplicate_rows = df[df.duplicated()]
    if not duplicate_rows.empty:
        print("Duplicate rows:")
        print(duplicate_rows)
        print()
    else:
        print("No duplicate rows found \n")
            
    # Print first 3 rows 
    print("First 3 rows:")
    display(df.head(3))
    print()

#Function to merge stops and trips and aggregate by route type, stop id, feed_key and agencies_name
def merge_and_aggregate_stops_and_trips(stoptimes_data, trips_data, agg_prefix=''):
    on_cols = ["trip_id", "feed_key"]
    how = 'left'
    joined_df = pd.merge(stoptimes_data, trips_data, on=on_cols, how=how, suffixes=('_stops','_trips'))
    
    groupby_cols=['route_type', 'stop_id', 'feed_key', 'name']
    agg_cols={
        f'n_trips_{agg_prefix}': ('trip_id', 'nunique'),
        f'n_routes_{agg_prefix}': ('route_id', 'nunique')
}
    aggregated_data = joined_df.groupby(groupby_cols).agg(**agg_cols).reset_index()
    return aggregated_data 


#Function to merge stops_geo and stop_times 
def merge_stops(stoptimes_data, stops_location_data, on_cols):
    joined_df = pd.merge(stoptimes_data, stops_location_data, on=on_cols)
    return joined_df

#Function to merge stoptimes for weekday, saturday and sunday 
def merge_stoptimes(stoptimes_weekday, stoptimes_sat, stoptimes_sun, merge_cols, final_cols):
    merged_df = pd.merge(stoptimes_weekday, stoptimes_sat, on=merge_cols, how="outer")
    merged_df = pd.merge(merged_df, stoptimes_sun, on=merge_cols, how="outer")
    
    merged_df=merged_df[final_cols]
    return merged_df

#Function to plot trips per stops and routes per stop
import matplotlib.pyplot as plt
def plot_histogram(data, column, title):
    data.pivot(columns='name', values=column).plot.hist(grid=True, bins=100, rwidth=0.9, log=True,
                                                             title=title)
    plt.xlabel('Trips per Stop')
    plt.ylabel('Number of Stops')
    plt.show()

if __name__ == "__main__":

    #  add all from notebook except checks/illustrations...
