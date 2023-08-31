import datetime
import dask.dataframe as dd
import numpy as np
import geopandas as gpd
import pandas as pd
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (
    PROJECT_CRS,
    SEGMENT_GCS,
    analysis_date,
)
from scripts import A1_sjoin_vp_segments

"""
Paths
"""
CONFIG_PATH = './scripts/config.yml'
STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")

"""
General Functions
"""
def count_trips_routes(df:pd.DataFrame):
    """
    Find the route/trip combination with the 
    highest number of n_trips. 
   
    Args:
        df can be any df (from flag_stage3 or merge_all_speeds)
    """
    cols_to_keep = ['shape_array_key','gtfs_dataset_name','gtfs_dataset_key', 'trip_id', 'n_trips']
    m1 = (df
     .sort_values(['n_trips'], ascending = False)
     .drop_duplicates(['shape_array_key'])
     [cols_to_keep]
         )
    return m1

def original_df_rows(original_df: pd.DataFrame, 
                     trip_id:str, 
                     shape_array_key:str) -> pd.DataFrame:
    """
    Look at the rows from the subsetted original df
    for one particular route/trip.
    """
    preview_cols = ['stop_sequence','stop_id','meters_elapsed','sec_elapsed',]
    
    df = (original_df[(original_df.trip_id == trip_id)
            & (original_df.shape_array_key == shape_array_key)]
            [preview_cols]
            .sort_values(['stop_sequence'])
           )
            
    return df

"""
Troubleshooting
Functions
"""
def merge_all_speeds(analysis_date:str) -> pd.DataFrame:
    """
    Merge avg_speeds_stop_segments and speed_stops parquets.
    
    Args:
        date: analysis date
    """
    STG5_FILE = STOP_SEG_DICT['stage5']
    
    # Open up avg speeds
    avg_speeds = pd.read_parquet(f"{SEGMENT_GCS}{STG5_FILE}_{analysis_date}.parquet")
    avg_speeds = avg_speeds.drop(columns=["geometry", "district", "district_name"])
    
    # Filter  for all day flags
    avg_speeds = avg_speeds[avg_speeds.time_of_day == 'all_day'].reset_index(drop = True)
    
    STG4_FILE = STOP_SEG_DICT['stage4']
    # Open up speeds
    speeds = pd.read_parquet(f"{SEGMENT_GCS}{STG4_FILE}_{analysis_date}")
    
    # Merge
    merge_cols = ['gtfs_dataset_key','shape_array_key', 'stop_sequence']
    m1 = pd.merge(avg_speeds, speeds, on = merge_cols, how = 'inner')
    
    m1 = m1.drop_duplicates().reset_index(drop = True)
    
    return m1

def categorize_by_percentile_pandas(
    df: pd.DataFrame, 
    column_percentile: str, 
    column_str: str) -> pd.DataFrame:

    # Find percentiles
    p5 = df[column_percentile].quantile(0.05).astype(float)
    p95 = df[column_percentile].quantile(0.95).astype(float)
    
    # Keep the most extremes of both ends across the entire dataframe
    def rate(row):
        if ((row[column_percentile] >= 0) and (row[column_percentile] <= p5)):
            return f"{column_str} is low"
        elif (row[column_percentile] >= p95):
               return f"{column_str} is high"
        else:
            return f"{column_str} is avg"
    
    # Apply flags
    df[f"{column_str}cat"] = df.apply(lambda x: rate(x), axis=1)
    
    # Clean
    df[f"{column_str}cat"] = df[f"{column_str}cat"].str.replace("_", "")
    
    return df  

def categorize_meters_speeds_pandas(original:pd.DataFrame)-> pd.DataFrame:
    """
    Categorize the meters and seconds elapsed
    rows as low/high/division by 0 across the entire 
    dataframe.
    
    Args:
        original: df from merge_all_speeds()
    """
    # Load original dataframe
    print(f"There are {len(original)} rows in the original dataframe") 
    
    # Categorize
    df1 = categorize_by_percentile_pandas(original, "meters_elapsed", "meters_")
    df2 = categorize_by_percentile_pandas(df1, "sec_elapsed", "sec_")
  
    # Find size of categories
    print(df2.groupby(['sec_cat','meters_cat']).size())
    
    def flag_round(row):
        if (row["meters_elapsed"] == 0) & (row["sec_elapsed"] == 0):
            return "division by 0"
        elif row["meters_cat"] == "meters is low":
            return "meters too low"
        elif row["sec_cat"] == "sec is high":
            return "seconds too high"
        else:
            return "ok"
        
    df2["flag"] = df2.apply(lambda x: flag_round(x), axis=1)
    print(df2.flag.value_counts()/len(df2)*100)
    
    return df2

def keep_only_zeroes(flagged:pd.DataFrame)-> pd.DataFrame:
    """
    Filter out for rows that have meters & sec elapsed by 0. 
    
    Args:
        flagged: df from categorize_meters_speeds_pandas()
    """
    # Filter out for only division by 0 
    df2 = flagged[(flagged.flag == 'division by 0')].reset_index(drop = True)
    
    # Print out some stuff of interest
    print(f"{flagged.trip_id.nunique()-df2.trip_id.nunique()} unique trips flagged.")
    print(f"{df2.shape_array_key.nunique()} routes flagged out of {flagged.shape_array_key.nunique()}.")
    print(f"{df2.shape_array_key.nunique()/flagged.shape_array_key.nunique() * 100} routes have 1+ row that has zeroes for meters/sec elapsed")
    print(f"{flagged.gtfs_dataset_name.nunique()-df2.gtfs_dataset_name.nunique()} operators are not flagged.")
    
    return df2

def problematic_trips_percentage(original:pd.DataFrame, 
                                 flagged:pd.DataFrame,
                                 most_least_populated: bool = True)->pd.DataFrame:
    """
    See the % of trips for a route that has 
    at least one row that is divided by 0.
    The lower the %, the better. 
    
    Args:
        original: df from merge_all_speeds()
        flagged: df from categorize_meters_speeds_pandas()
    """
    # m1 = merge_all_speeds(analysis_date)
    # Number of trips that have at least one row that was divided by 0 
    # for this shape array key
    df1 = flagged.groupby(['shape_array_key']).agg({'trip_id':'nunique'}).rename(columns = {'trip_id':'trips_with_zero'}).reset_index()
    
    # Original number of trips
    df2 = original.groupby(['shape_array_key']).agg({'trip_id':'nunique'}).rename(columns = {'trip_id':'all_trips'}).reset_index()
    
    # Merge
    m1 = pd.merge(df2, df1, how = "left", on = 'shape_array_key')
    
    # Clean
    m1['percent_of_trips_with_problematic_rows'] = (m1.trips_with_zero/m1.all_trips * 100).fillna(0)
    m1.trips_with_zero = m1.trips_with_zero.fillna(0)
    
    print(f"{len(m1[m1.percent_of_trips_with_problematic_rows == 0])/m1.shape_array_key.nunique() * 100}% of routes have 1+ division by 0 row")
    return m1

def route_most_populated(flagged:pd.DataFrame)-> pd.DataFrame:
    """
    For each route, the "quality" of vehicle positions varies by trip.
    Find the trip with the highest percentage of "ok" rows. Ok is defined
    as a row with non-zero values populated for meters_elapsed and 
    sec_elapsed
    
    Args:
        flagged: df from categorize_meters_speeds_pandas()
    """
    # First aggregation to count number of stops by flag
    agg1 = (flagged
        .groupby(['gtfs_dataset_key','shape_array_key','trip_id','flag'])
        .agg({'stop_sequence':'nunique'})
        .rename(columns = {'stop_sequence':'number_of_rows'})
        .reset_index()
       )
    
    # Create separate cols for the number of rows that are ok and rows that are division by 0
    # https://stackoverflow.com/questions/49161120/set-value-of-one-pandas-column-based-on-value-in-another-column
    agg1['division_by_zero'] = None
    agg1['ok'] = None
    agg1['division_by_zero'] = np.where(agg1.flag == 'division by 0', agg1.number_of_rows, agg1.division_by_zero)
    agg1['ok'] = np.where(agg1.flag != 'division by 0', agg1.number_of_rows, agg1.ok)
    agg1['division_by_zero'] = agg1['division_by_zero'].fillna(0)
    agg1['ok'] = agg1['ok'].fillna(0)
    
    # Aggregate again to simplify the df 
    agg1 = agg1.drop(columns = ['flag','number_of_rows'])
    agg2 = (agg1
            .groupby(['gtfs_dataset_key','shape_array_key','trip_id'])
            .agg({'division_by_zero':'sum','ok':'sum'})
            .reset_index()
           )
    
    # Find total rows for that trip
    agg2['total_rows'] = agg2.division_by_zero + agg2.ok
    
    # Find total % of rows that are ok
    agg2['percent_of_ok_rows'] = (agg2.ok/agg2.total_rows * 100)
    
    # Filter out for rows that are equal to 1
    agg2 = agg2[agg2.total_rows != 1].reset_index(drop = True)
    
    # Only keep the route/trip with the highest % of ok rows
    agg2 = (agg2
            .sort_values(['gtfs_dataset_key','shape_array_key','percent_of_ok_rows'], ascending = False)
            .drop_duplicates(['shape_array_key', 'gtfs_dataset_key'])
            .reset_index(drop = True)
           )
    
    return agg2

def flagging_stage(analysis_date:str)->pd.DataFrame:
    """
    This function loads the various troubleshooting dfs in 
    one go.
    
    Args:
        analysis_date: date of files
    """
    start = datetime.datetime.now()
    print(start)
    # Load original but merged avg speeds and speed_stop_segments
    original_df = merge_all_speeds(analysis_date)
    
    # Flag original df
    flagged_df = categorize_meters_speeds_pandas(original_df)
    
    # Filter for only ows that are only divided by 0
    zeroes_only_df = keep_only_zeroes(flagged_df)
    
    # Find % trips with 1+ row that has a division by 0 for a route
    problematic_trips_df = problematic_trips_percentage(original_df, zeroes_only_df)
    
    # Find the trip for a route that has the highest % of ok rows
    most_populated_trip_df = route_most_populated(flagged_df)
    
    end = datetime.datetime.now()
    print(f"Took {end-start}")
    return flagged_df, zeroes_only_df, problematic_trips_df, most_populated_trip_df

"""
Investigation
Stage1
"""
def load_vp_stage3(flagged_df:pd.DataFrame, date:str) -> pd.DataFrame:
    """
    Load vp_pared_stops
    
    Args:
        flagged_df: df from categorize_meters_speeds_pandas()
        date: analysis date
    """
    # Subset to filter vp_pared_stops.
    shape_array_keys = flagged_df.shape_array_key.unique().tolist()
    stop_seq = flagged_df.stop_sequence.unique().tolist() 
    trip_id = flagged_df.trip_id.unique().tolist() 
    gtfs_dataset_key = flagged_df.gtfs_dataset_key.unique().tolist() 
    
    FILE = STOP_SEG_DICT['stage3']
    
    vp = pd.read_parquet(f"{SEGMENT_GCS}{FILE}_{date}",
        filters = [[('shape_array_key', "in", shape_array_keys),
                   ('stop_sequence', 'in', stop_seq), 
                   ('trip_id', 'in', trip_id), 
                   ('gtfs_dataset_key', 'in', gtfs_dataset_key)]],)
    
    # Merge to capture original df information and filter down further
    vp2 = pd.merge(flagged_df, vp, how = "inner", on = ['gtfs_dataset_key', 'trip_id','stop_sequence','shape_array_key','gtfs_dataset_name'])
    
    return vp2

def stage3_repeated_timestamps(stage3_df:pd.DataFrame)-> pd.DataFrame:
    """
    Look at how many times a time stamp is repeated a route-trip-location.
    Each of these 3 combos should have a different time for each 
    stop sequence or else the vehicle is not moving.
    
    Args:
        stage3_df: df from load_vp_stage3()
    """
    agg = (stage3_df
     .groupby(['shape_array_key','trip_id', 'location_timestamp_local'])
     .agg({'stop_sequence':'nunique'})
     .reset_index()
     .rename(columns = {'stop_sequence':'number_of_repeated_timestamps'})
    )
    
    # Only keep timestamps that are repeated more than once
    agg = (agg[agg.number_of_repeated_timestamps > 1]).reset_index(drop = True)

    return agg

def stage3_repeated_locations(stage3_df:pd.DataFrame)-> pd.DataFrame:
    """
    Look at how many times a time stamp is repeated for a stop-trip-route combo.
    Each of these 3 combos should have a different location for each 
    stop sequence or else the vehicle is not changing moving.
    
    Args:
        stage3_df: df from load_vp_stage3()
    """
    # Concat x and y into a string
    stage3_df['pair'] = stage3_df.x.astype(str) + '/' + stage3_df.y.astype(str)
    
    # Count number of different stops that reference the same location
    agg = (stage3_df
     .groupby(['shape_array_key','trip_id','pair'])
     .agg({'stop_sequence':'nunique'})
     .reset_index()
     .sort_values('stop_sequence', ascending = False)
     .rename(columns = {'stop_sequence':'number_of_repeated_locs'})               
    )

    # Only keep locations that are repeated more than once
    agg = agg[agg.number_of_repeated_locs != 1].reset_index(drop = True)
    
    return agg

def flag_stage3(flagged_df:pd.DataFrame, date:str) -> pd.DataFrame:
    """
    Flag the errors in stage3.
    
    Args:
        flagged_df: df from categorize_meters_speeds_pandas()
        date: analysis date
    """
    start = datetime.datetime.now()
    print(start)
    
    # Relevant rows from Vehicle Positions
    vp = load_vp_stage3(flagged_df, date)
    
    # Find repeated timestamps.
    multi_timestamps = stage3_repeated_timestamps(vp)
    
    # Find repeated locations
    multi_locs = stage3_repeated_locations(vp)
    
    # Merge
    timestamps_merge_cols = ['shape_array_key','trip_id','location_timestamp_local']
    loc_merge_cols =  ['shape_array_key','trip_id','pair']
    
    # Want everything found in vehicle positions, so do left merges
    m1 = (vp
          .merge(multi_timestamps, how="left", on= timestamps_merge_cols)
          .merge(multi_locs, how="left", on=loc_merge_cols)
         )
    
    drop_cols = ['vp_idx','x','y','hour']
    
    # Drop columns
    m1 = m1.drop(columns = drop_cols)
    
    # Flag
    def flag(row):
        if (row["number_of_repeated_timestamps"] > 1) & (row["number_of_repeated_locs"] > 1):
            return "repeated timestamps & locations"
        elif (row["number_of_repeated_timestamps"] > 1):
            return "repeated timestamps"
        elif (row["number_of_repeated_locs"] > 1):
            return "repeated locations"
        else:
            return "check in stage 2"
        
    m1["stage3_flag"] = m1.apply(lambda x: flag(x), axis=1)
    m1 = m1.drop_duplicates().reset_index(drop = True)
    
    print(m1.stage3_flag.value_counts())
    
    check_in_stage2 = m1[m1.stage3_flag == "check in stage 2"]
    print(f"Have to check {len(check_in_stage2)/len(m1) * 100} % of rows in stage 2")
    
    check_in_stage2 = check_in_stage2.drop_duplicates().reset_index(drop = True)
    end = datetime.datetime.now()
    print(f"Took {end-start}")
    return m1

"""
Investigation
Stage2
"""
def import_stage_2(date:str, route:str, stop_sequence:str):
    FILE = STOP_SEG_DICT['stage2']
    df = pd.read_parquet(
            f"{SEGMENT_GCS}vp_sjoin/{FILE}_{date}",
            filters = [[('shape_array_key', "==", route),
                       ('stop_sequence', "==", stop_sequence)]],
        )
    return df

def import_unique_trips(gtfs_key:str, trip: str, route:str):
    """
    Read vp_usable file for one 
    trip/route/operator and find the unique trips.
    """
    FILE = STOP_SEG_DICT['stage1']
    vp_trips = A1_sjoin_vp_segments.add_grouping_col_to_vp(
        f"{FILE}_{analysis_date}",
        analysis_date,
       ["shape_array_key"]
    )
    
    # Filter to just one trip/route/operator
    df = vp_trips[(vp_trips.gtfs_dataset_key == gtfs_key)
                    & (vp_trips.shape_array_key == route)
                    & (vp_trips.trip_id == trip)].reset_index(drop = True)
    return df

def import_vehicle_positions(unique_trips:pd.DataFrame, 
                             gtfs_key:str,
                             trip_id:str)-> gpd.GeoDataFrame:
    """
    Find ALL points for the trip.
    
    Args:
        unique_trips: df from import_unique_trips()
    """
    FILE = STOP_SEG_DICT['stage0']
    
    vp = helpers.import_vehicle_positions(
            SEGMENT_GCS,
            f"{FILE}_{analysis_date}/",
            "gdf",
            filters = [[("gtfs_dataset_key", "==", gtfs_key),
                      ('trip_id', '==', trip_id)]],
            columns = ["gtfs_dataset_key", "trip_id","geometry"],
            partitioned = False
        )
    vp = vp.compute()

    vp = vp.merge(unique_trips, on = ["gtfs_dataset_key", "trip_id"],
            how = "inner"
        )
    return vp

def import_segments1(flagged_df: pd.DataFrame, 
                    route:str, 
                    gtfs_key:str, 
                    trip_id:str) -> gpd.GeoDataFrame:
    """
    Import cut segments and colorcode  them based on 
    whether or not it has 1+ rows that is divided by 0.
    Cavaet: even if a segment records only 1 row that is divided by 0,
    it will be color coded as so.
    
    Args:
        flagged_df: result from df from categorize_meters_speeds_pandas()
    """
    # Load in ALL segments, flag them.
    FILE = STOP_SEG_DICT['segments_file']
    gdf = gpd.read_parquet(f"{SEGMENT_GCS}{FILE}_{analysis_date}.parquet",
                           filters = [[("shape_array_key", "==", route),
                                      ("gtfs_dataset_key", "==", gtfs_key),
                                     ]]).to_crs(PROJECT_CRS)
    
    gdf["geometry_buffered"] = gdf.geometry.buffer(35)
    gdf = gdf.set_geometry('geometry_buffered')
    
    """
    # Distinguish between "correct" and "incorrect" seq
    # A sequence can be incorrect even if just one row is "divided by 0"
    incorrect_segments = flagged_df[
        (flagged_df.shape_array_key == route)
        & (flagged_df.gtfs_dataset_key == gtfs_key)
        & (flagged_df.trip_id == trip_id)
    ]
    incorrect_segments_list = incorrect_segments.stop_sequence.unique().tolist()
    incorrect_segments_filtered = gdf[gdf.stop_sequence.isin(incorrect_segments_list)].reset_index(drop = True)
    incorrect_segments_filtered['flag'] = 'contains 0m/0sec'
    
    # Filter for correct segments using 
    correct_segments = flagged_df[~flagged_df.stop_sequence.isin(incorrect_segments_list)]
    correct_segments_list = correct_segments.stop_sequence.unique().tolist()
    correct_segments_filtered = gdf[gdf.stop_sequence.isin(correct_segments_list)].reset_index(drop = True)
    correct_segments_filtered['flag'] = 'does not contain 0m/0sec'
    
    final = pd.concat([correct_segments_filtered, incorrect_segments_filtered])
    """
    return gdf

def import_segments(flagged_df: pd.DataFrame, 
                    route:str, 
                    gtfs_key:str, 
                    trip_id:str) -> gpd.GeoDataFrame:
    """
    Import cut segments and colorcode  them based on 
    whether or not it has 1+ rows that is divided by 0.
    Cavaet: even if a segment records only 1 row that is divided by 0,
    it will be color coded as so.
    
    Args:
        flagged_df: result from df from categorize_meters_speeds_pandas()
    """
    # Load in ALL segments, flag them.
    FILE = STOP_SEG_DICT['segments_file']
    gdf = gpd.read_parquet(f"{SEGMENT_GCS}{FILE}_{analysis_date}.parquet",
                           filters = [[("shape_array_key", "==", route),
                                      ("gtfs_dataset_key", "==", gtfs_key),
                                     ]]).to_crs(PROJECT_CRS)
    
    gdf["geometry_buffered"] = gdf.geometry.buffer(35)
    gdf = gdf.set_geometry('geometry_buffered')
    
    return gdf

def find_first_last_points(route:str, trip:str, gtfs_key:str)-> gpd.GeoDataFrame:
    """
    Load gdf with only the first and last points pared. 
    """
    FILE = STOP_SEG_DICT['stage3']
    
    df = pd.read_parquet(f"{SEGMENT_GCS}{FILE}_{analysis_date}",
        filters = [[('shape_array_key', "==", route),
                  
                   ('trip_id', "==", trip), 
                   ('gtfs_dataset_key', '==', gtfs_key)]],)
    
    gdf =  gpd.GeoDataFrame(
        df, 
        geometry = gpd.points_from_xy(df.x, df.y, crs = "EPSG:4326")
    ).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    gdf = gdf[['geometry','stop_sequence']]
    
    return gdf

def sjoin_vp_segments(segments: gpd.GeoDataFrame,
                      vp_gdf: gpd.GeoDataFrame)-> gpd.GeoDataFrame:
    """
    Sjoin all the points with the relevant segments
    """
    vp_in_seg = gpd.sjoin(
        vp_gdf,
        segments,
        how = "inner",
        predicate = "within"
    )
    
    return vp_in_seg

def display_maps(all_points: gpd.GeoDataFrame, 
                 first_last_points: gpd.GeoDataFrame,
                 segments: gpd.GeoDataFrame,
                 sjoin_results: gpd.GeoDataFrame):
    """
    Create 3 maps: one to display all points, one to display
    the sjoined points, one to display first and last points that are
    automatically kept.
    
    Args:
        all_points: gdf from import_vehicle_positions()
        first_last_points: gdf from find_first_last_points()
        segments: gdf from import_segments()
        sjoin_results: gdf from sjoin_vp_segments()
    """
    # segments = segments[~segments.geometry_buffered.is_empty]
    # segments = segments[~(segments.is_empty | segments.isna())]
    
    #  Deleted flag
    base1 = segments.explore(cmap= 'tab10', height = 400, width = 600, name = 'segments')
    all_points_map = all_points.explore(m = base1, color = 'red',style_kwds = {'weight':6}, name= 'points')
    
    print('ALL POINTS')
    display(all_points_map) 
     
    # Have to use a different base geometry for sjoin 
    sjoin_points = sjoin_results.set_geometry('geometry_left')
    sjoin_segments = sjoin_results.set_geometry('geometry_right')
    sjoin_segments.geometry_right = sjoin_segments.geometry_right.buffer(35)
    base3 = sjoin_segments.explore(cmap= 'tab10', height = 400, width = 600, name = 'segments')
    sjoin_map = sjoin_points.explore(m = base3, color = 'orange',style_kwds = {'weight':6},  name= 'points')
    
    print('SJOIN')
    display(sjoin_map)
    
    base2 = segments.explore(cmap= 'tab10', height = 400, width = 600, name = 'segments')
    first_last_map = first_last_points.explore(m = base2, color = 'pink',style_kwds = {'weight':6},height = 400, width = 600,)
    
    print('FIRST AND LAST')
    display(first_last_map)

def stage2_trouble_shooting(flagged_df:pd.DataFrame,
                            date:str, 
                            route:str, 
                            trip:str, 
                            gtfs_key:str):
    
    """
    Putting together all the functions for stage 2
    """
    unique_trips = import_unique_trips(gtfs_key, trip, route)
    
    # Find all recorded vps
    vehicle_positions = import_vehicle_positions(unique_trips, gtfs_key, trip)
    
    # Flag segments, whether one row contains one or more 0/0 division or not
    flagged_segments = import_segments(flagged_df, route, gtfs_key, trip)
    
    # Find first and last pt kept
    first_last = find_first_last_points(route, trip, gtfs_key)
    
    # Sjoin 
    sjoin_results = sjoin_vp_segments(flagged_segments,vehicle_positions)
    
    # Display maps
    display_maps(vehicle_positions,first_last,flagged_segments,sjoin_results)
    