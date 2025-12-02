import geopandas as gpd
import pandas as pd
from siuba import *
import numpy as np

from calitp_data_analysis.sql import to_snakecase
from calitp_data_analysis import utils
from calitp_data_analysis import get_fs
import gcsfs as fs
fs = get_fs()

import os
import shutil

import altair as alt
import folium

from IPython.display import HTML
from calitp_data_analysis import calitp_color_palette as cp


gcs_path = "gs://calitp-analytics-data/data-analyses/big_data/hta/"


def read_in_and_prep_replica_data(file_name, shape_data, file_type):
    
    ### read in the replica study data
    df = to_snakecase( pd.read_csv(f"{gcs_path}{file_name}"))    
    
    ### read in the census blockgroup shape data
    with get_fs().open(f"{gcs_path}{shape_data}") as f:
        blkgr = to_snakecase(gpd.read_file(f))
    
    ##get the county and state info
    blkgr["name_county"]  = blkgr['geoname'].str.rstrip(', ').str.split(', ').str[1] 
    blkgr["name_state"]  = blkgr['geoname'].str.strip().str[-3:-1]
    
    ### get 
    blkgr_map = dict(zip(blkgr['geoname'], 
                          blkgr['geometry']))
    
    ### set the geometry. choose the right blockgroup based on the data study type
    df["geometry"] = np.nan
    
    ### i.e. if the data has CalPoly as the destination, then set the geometry as the origin and vice versa
    if file_type == "to_cp":
            
        df['geometry'] = df['geometry'].fillna(df['origin_bgrp_2020'].map(blkgr_map))
        
    elif file_type== "from_cp":
        df['geometry'] = df['geometry'].fillna(df['destination_bgrp_2020'].map(blkgr_map))

    df = df.set_geometry("geometry")

    df = df.set_crs(4326)
    
    return df


def aggregate_destination_station_geometries(df_all_stations, origin_stations_list):
    
    for station in origin_stations_list:
        station_destinations = df_all_stations >> filter(_.station_name != station)
        station_destinations_deduplicated = station_destinations.drop_duplicates(subset=['geoname'], keep='first')

        utils.make_zipped_shapefile(station_destinations_deduplicated, f"station_destinations/origin_{station}_destinations_network_wide.zip")
        # station_destinations.to_file(f"{gcs_path}station_destinations_combined/{station}_destinations.shp", driver="ESRI Shapefile")

        print(f"Sucessfully exported {station} destinations to gcs")

    ### remove locations folder
    ### this function also puts a location in the root of the folder
    folder_to_delete = "./station_destinations"  # Replace with the actual path to your folder

    if os.path.isdir(folder_to_delete):
        try:
            shutil.rmtree(folder_to_delete)
            print(f"The folder '{folder_to_delete}' and its contents have been successfully removed.")
        except OSError as e:
            print(f"Error: {folder_to_delete} : {e.strerror}")
    else:
        print(f"The folder '{folder_to_delete}' does not exist.")
        

        

def calc_travel_info(df):
    
    mean_min = df.trip_duration_minutes.mean()
    median_min = df.trip_duration_minutes.median()
    
    mean_miles = df.trip_distance_miles.mean()
    median_miles = df.trip_distance_miles.median()
    
    max_miles = df.trip_distance_miles.max()
    max_min = df.trip_duration_minutes.max()
    
    return mean_min, median_min, mean_miles, median_miles, max_miles, max_min

# def calc_auto_travel_info(df):
#     df_auto = df[df.primary_mode=="private_auto"]
    
#     auto_mean_min = df_auto.trip_duration_minutes.mean()
#     auto_median_min = df_auto.trip_duration_minutes.median()
    
#     auto_mean_miles = df_auto.trip_distance_miles.mean()
#     auto_median_miles = df_auto.trip_distance_miles.median()
    
#     auto_max_min = df_auto.trip_duration_minutes.max()
#     auto_max_miles = df_auto.trip_distance_miles.max()
    
#     return auto_mean_min, auto_median_min, auto_mean_miles, auto_median_miles, auto_max_min, auto_max_miles


# def calc_transit_travel_info(df):
#     df_auto = df[df.primary_mode=="public_transit"]
    
#     transit_mean_min = df_auto.trip_duration_minutes.mean()
#     transit_median_min = df_auto.trip_duration_minutes.median()
    
#     transit_mean_miles = df_auto.trip_distance_miles.mean()
#     transit_median_miles = df_auto.trip_distance_miles.median()
    
#     transit_max_miles = df_auto.trip_distance_miles.max()
#     transit_max_min = df_auto.trip_duration_minutes.max()
    
#     return transit_mean_min, transit_median_min, transit_mean_miles, transit_median_miles, transit_max_miles, transit_max_min


def get_top_and_bottom_tract_counts(df, top_least, all_trips):
    tract_counts = df['destination_tract_station_area'].value_counts().reset_index()
    tract_counts.columns = ['destination_tract_station_area', 'count']
    
    if (top_least == ("top")):
        top_name1 = tract_counts.iloc[0, 0]
        # top_num1 = (tract_counts.iloc[0, 1])/all_trips

        # top_name2 = tract_counts.iloc[1, 0]
#         top_num2 = (tract_counts.iloc[1, 1])/all_trips

        # top_name3 = tract_counts.iloc[2, 0]
#         top_num3 = (tract_counts.iloc[2, 1])/all_trips
        
        return top_name1, #top_name2, #top_name3
    
    if (top_least == ("least")):
        bottom_name1 = tract_counts.iloc[(len(tract_counts)-1), 0]
        # bottom_num1 = tract_counts.iloc[(len(tract_counts)-1), 1]

        # bottom_name2 = tract_counts.iloc[(len(tract_counts)-2), 0]
#         # bottom_num2 = tract_counts.iloc[(len(tract_counts)-2), 1]

#         bottom_name3 = tract_counts.iloc[(len(tract_counts)-3), 0]
#         # bottom_num3 = tract_counts.iloc[(len(tract_counts)-3), 1]
        
        return bottom_name1, #bottom_name2, #bottom_name3
    
    
def get_mode_split(df):
    
    ##get list of unique modes that appear in the Replica Studio results

    mode_list = list(df.primary_mode.unique())
    
    mode_pcts = []
    
    for mode in mode_list:
        
        df_copy = df.copy()
        
        df_name_ = df.loc[0, 'trip_type']
        
        df_mode_subset = df_copy[df_copy["primary_mode"] == mode ]
        # n_blkgr_dest = (df_copy.destination_bgrp_2020.nunique())
        # n_blkgr_origin = (df_copy.origin_bgrp_2020.nunique())
        n_trips = len(df_mode_subset)
        pct_trips = ((len(df_mode_subset)) / (len(df_copy)))

        
        mode_pcts.append({
            'trip_type': df_name_,
            'mode': mode,
            'pct_trips': pct_trips, 
            'total_trips': n_trips,
            # 'n_blkgrs_dest': n_blkgr_dest,
            # 'n_blkgrs_origin': n_blkgr_origin,
        })
        
    mode_pcts_summary = pd.DataFrame(mode_pcts)
    
    return mode_pcts_summary


def return_time_metrics(df, time_start_col, time_end_col):
    
    mode_col = "primary_mode"
    
    df['trip_start_time_test'] = df[time_start_col].apply(pd.to_datetime, errors="coerce")
    df['trip_end_time_test'] = df[time_end_col].apply(pd.to_datetime, errors="coerce")
    
    df_agg = (
        df.groupby(mode_col).agg(

        trip_start_min=('trip_start_time_test', 'min'),
        trip_start_max=('trip_start_time_test', 'max'),
        trip_start_median=('trip_start_time_test', 'median'),
        trip_start_mean=('trip_start_time_test', 'mean'),

        trip_end_min=('trip_end_time_test', 'min'),
        trip_end_max=('trip_end_time_test', 'max'),
        trip_end_median=('trip_end_time_test', 'median'),
        trip_end_mean=('trip_end_time_test', 'mean'),

        trip_duration_minutes_min=('trip_duration_minutes', 'min'),
        trip_duration_minutes_max=('trip_duration_minutes', 'max'),
        trip_duration_minutes_median=('trip_duration_minutes', 'median'),
        trip_duration_minutes_mean=('trip_duration_minutes', 'mean'),

        trip_duration_miles_min=('trip_distance_miles', 'min'),
        trip_duration_miles_max=('trip_distance_miles', 'max'),
        trip_duration_miles_median=('trip_distance_miles', 'median'),
        trip_duration_miles_mean=('trip_distance_miles', 'mean'),
        ).reset_index())

    df_agg.columns = [col.replace('_', ' ').title() for col in df_agg.columns]

    df_agg['Primary Mode'] = df_agg[f'Primary Mode'].apply(lambda x: x.replace('_', ' ').title())

    time_melt =  (pd.melt(
        df_agg,
        id_vars=["Primary Mode"],
        value_vars=[ 'Trip Start Min', 'Trip Start Max', 'Trip Start Median', 'Trip Start Mean', 
                   'Trip End Min', 'Trip End Max', 'Trip End Median', 'Trip End Mean',],
        var_name="Metric",
        value_name="Time"))
    
    time_melt_duration =  (pd.melt(
        df_agg,
        id_vars=["Primary Mode"],
        value_vars=['Trip Duration Minutes Min', 'Trip Duration Minutes Max', 'Trip Duration Minutes Median', 'Trip Duration Minutes Mean'],
        var_name="Metric",
        value_name="Value"))
    
    time_melt['Time'] = time_melt['Time'].apply(pd.to_datetime, errors="coerce")
    
    return time_melt, time_melt_duration



def return_mode_map(df, routes_df, mode_list, trip_type):

    for mode in mode_list:
        display(HTML(f"<h3>{(mode.title())} Trips {trip_type.replace('_', ' ').title()} Cal Poly</h3>"))

        df_susbet = (df[df["primary_mode"]==mode])

        if len(df_susbet)==0:
            display(HTML("<strong>**No trips present**</strong>"))

        else:
            m = df_susbet.explore(column="n_trips", 
                        scheme="NaturalBreaks",
                         k=10,
                        cmap="YlOrRd")
            m = routes_df.explore(m=m, column="Route Name", name="HTA Routes", tooltip=["Agency","Route Name", "SHN Route"])
            folium.LayerControl().add_to(m)

            display(m)
            


#### NEED TO REFACTOR
### putting it all together
def return_score_summary(df_list):

    results = []

    for df in df_list:
        
        trip_type = df.loc[0, 'trip_type']
        
        auto_df = (df[df.primary_mode=="private_auto"])
        transit_df = (df[df.primary_mode=="public_transit"])
        walking_df = (df[df.primary_mode=="walking"])

        all_trip_count = len(df)

        n_total_trips = len(df)
        n_private_auto_trips = len(auto_df)
        pct_private_auto_trips = ((len(auto_df)) / (len(df)))
        n_public_transit_trips = (len(transit_df))
        pct_public_transit_trips = ((len(transit_df)) / (len(df)))
        
        n_walking_trips = (len(walking_df))
        pct_walking_trips = ((len(walking_df)) / (len(df)))

        auto_mean_min, auto_median_min, auto_mean_miles, auto_median_miles, auto_max_min, auto_max_miles = calc_travel_info(auto_df)
        transit_mean_min, transit_median_min, transit_mean_miles, transit_median_miles, transit_max_miles, transit_max_min = calc_travel_info(transit_df)
        walking_mean_min, walking_median_min, walking_mean_miles, walking_median_miles, walking_max_miles, walking_max_min = calc_travel_info(walking_df)

                ## set up the table for all the results
        results.append({
                        'trip_type': trip_type,
                        'total_trips': n_total_trips,
                        'n_auto_trips': n_private_auto_trips,
                        'pct_auto_trips': pct_private_auto_trips,
                        'n_tranist_trips': n_public_transit_trips,
                        'pct_transit_trips': pct_public_transit_trips,
                        'n_walking_trips': n_walking_trips,
                        'pct_walking_trips': pct_walking_trips,
            
                        'auto_mean_minutes': auto_mean_min,
                        'auto_median_minutes': auto_median_min,
                        'auto_max_minutes': auto_max_min, 
                        'auto_mean_miles': auto_mean_miles,
                        'auto_median_miles': auto_median_miles,
                        'auto_max_miles': auto_max_miles,
            
                        'transit_mean_minutes': transit_mean_min,
                        'transit_median_minutes': transit_median_min,
                        'transit_max_minutes': transit_max_min,
                        'transit_mean_miles': transit_mean_miles,
                        'transit_median_miles': transit_median_miles,
                        'transit_max_miles':transit_max_miles,
                        
                        'walking_mean_minutes': walking_mean_min,
                        'walking_median_minutes': walking_median_min,
                        'walking_max_minutes': walking_max_min,
                        'walking_mean_miles': walking_mean_miles,
                        'walking_median_miles': walking_median_miles,
                        'walking_max_miles': walking_max_miles,

                        })

    result_summary = pd.DataFrame(results)   
    
    return result_summary
