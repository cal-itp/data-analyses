import geopandas as gpd
import numpy as np
import pandas as pd
import dask.dataframe as dd
import datetime 

from calitp_data_analysis.sql import to_snakecase
import _threshold_utils as threshold_utils
from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, RT_SCHED_GCS, analysis_date

# Graphs
import altair as alt
from shared_utils import calitp_color_palette as cp

import gcsfs
fs = gcsfs.GCSFileSystem()

import intake
catalog = intake.open_catalog("./catalog.yml")

"""
RT v. Scheduled Utils
"""
def count_rt_min(df):
    """
    Find total RT minute coverage
    for each trip-operator
    """
    # Extract hour and minute
    df['hour'] = df.location_timestamp.dt.hour
    df['minute'] = df.location_timestamp.dt.minute
    
    subset_cols = ['gtfs_dataset_key', 'trip_id','hour', 'minute']
    
    # Drop duplicates
    df = df.drop_duplicates(subset = subset_cols).reset_index(drop = True)
    
    # Count # of rows to get minutes of RT data. 
    df = (df
          .groupby(['gtfs_dataset_key','trip_id'])
          .agg({'hour':'count'})
          .reset_index()
          .rename(columns = {'hour':'total_rt_min_coverage'})
         )
    return df

def find_start_end_times(df, groupby_cols:list):
    """
    Find the max and min location stamp 
    to determine start and end date. 
    """
    start_end = (df.groupby(groupby_cols)
        .agg({"location_timestamp": [ "min", "max"]})
        .reset_index()
        .rename(columns = {'min':'start', 'max':'end'})
      ) 
    
    # Clean up columns
    start_end.columns = start_end.columns.droplevel()
    start_end.columns.values[0] = "_gtfs_dataset_name"
    start_end.columns.values[1] = "gtfs_dataset_key"
    start_end.columns.values[2] = "trip_id"
    
    return start_end

def naive_timezone(df):
    """
    Convert UTC to PST to Naive. 
    """
    PACIFIC_TIMEZONE = "US/Pacific"
    
    # Grab datetime columns
    dt_cols = [col for col in df.columns if df[col].dtype == 'datetime64[ns, UTC]']
    
    for col in dt_cols:
        df[col] = df[col].dt.tz_convert(PACIFIC_TIMEZONE).apply(lambda t: t.replace(tzinfo=None))
        
    return df 

def convert_timestamp_to_hrs_mins(
    df: pd.DataFrame, 
    timestamp_col: list,
    minutes: bool = True,
) -> pd.DataFrame: 
    """
    Convert datetime col into minutes or seconds.
    """
    if minutes:
        for c in timestamp_col:
            df = df.assign(
                time_min = ((df[c].dt.hour * 60) + 
                                (df[c].dt.minute) + 
                                (df[c].dt.second/60)
                           ),
            ).rename(columns = {"time_min": f"{c}_minutes"})
    
    else:
        for c in timestamp_col:
            df = df.assign(
                time_sec = ((df[timestamp_col].dt.hour * 3_600) + 
                                (df[timestamp_col].dt.minute * 60) + 
                                (df[timestamp_col].dt.second)
                           ),
            ).rename(columns = {"time_sec": f"{timestamp_col}_sec"})

    return df

def find_metrics(date:str):
    """
    Find metrics such as number of pings,
    start and end time, and RT coverage for a trip.
   
    date (str): the analysis date from `segment_speed_utils.project_vars`
    """
    group_cols = ["_gtfs_dataset_name", "gtfs_dataset_key", "trip_id"]
    
    # Load in file.
    ddf = helpers.import_vehicle_positions(
        gcs_folder = f"{SEGMENT_GCS}vp_sjoin/",
        file_name = f"vp_route_segment_{date}/",
        file_type = "df",
        columns = ["gtfs_dataset_key", "_gtfs_dataset_name", 
                   "trip_id", "route_dir_identifier",
                   "location_timestamp"],
        partitioned = True
    ).repartition(partition_size="85MB")
    
    df = ddf.compute()
    
    # Find number of RT minutes
    rt_min_avail = count_rt_min(df)
    
    # Find start and end time for a trip
    start_end = find_start_end_times(df, group_cols)
    
    # Find number of pings by 
    # counting nunique location_timestamps
    pings = (df.groupby(group_cols)
          .agg({'location_timestamp':'nunique'})
          .reset_index()
          .rename(columns = {'location_timestamp':'trip_ping_count'})
         )
    
    # Find number of trips for each operator
    trips_ops = (df
                .groupby(["gtfs_dataset_key", "_gtfs_dataset_name"])
                .agg({'trip_id':'nunique'})
                .reset_index()
                .rename(columns = {'trip_id':'rt_trip_counts_by_operator'})
        )
    
    m1 = (start_end.merge(pings, how="inner", on = group_cols)
                   .merge(rt_min_avail, how = "inner", on = ['gtfs_dataset_key','trip_id'])
                   .merge(trips_ops, how = "inner", on = ['gtfs_dataset_key', '_gtfs_dataset_name']
         ))
    
    # Convert to naive timezone
    m1 = naive_timezone(m1)
    
    # Convert timestamp to minutes
    m1 = convert_timestamp_to_hrs_mins(m1, ['start','end'])
    
    # Find actual trip times
    m1['actual_trip_duration_minutes'] = (m1['end_minutes']-m1['start_minutes'])
    return m1

def merge_schedule_vp(vp_df, date: str):
    """
    Merge scheduled data with RT data. 
    Keeping this as a separate function  b/c
    it could be useful to have the merge indicator
    column to show how many trips appear in both
    vs. one dataset.
    
    vp_df: the dataframe produced by `find_metrics`
    date (str): the analysis date from `segment_speed_utils.project_vars`
    """
    # Load scheduled trips
    scheduled_trips = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(analysis_date =date, 
    keep_trip_cols = ["feed_key", "trip_id", "service_hours"])
    
    # Convert trip hours to minutes
    scheduled_trips['scheduled_service_minutes'] = scheduled_trips.service_hours * 60
    
    # Merge scheduled with RT. 
    m1 = pd.merge(vp_df, scheduled_trips, how="outer", on=["gtfs_dataset_key", "trip_id"], indicator=True)
    
    return m1

def trip_duration_categories(row):
    if row.actual_trip_duration_minutes < 31:
        return "0 - 30 minutes"
    elif 30 < row.actual_trip_duration_minutes < 61:
        return "31-60 minutes"
    elif 60 < row.actual_trip_duration_minutes < 91:
        return "61-90 minutes"
    else:
        return "90+ minutes"
    
def rt_data_proportion(row):
    if  row.rt_data_proportion_percentage < 21:
        return "0-20%"
    elif 20 < row.rt_data_proportion_percentage < 41:
        return "21-40%"
    elif 40 < row.rt_data_proportion_percentage < 61:
        return "41-60%"
    elif 60 < row.rt_data_proportion_percentage < 80:
        return "61-80%"
    else:
        return "81-100%"    
    
def pings_categories(row):
    if 2.7 < row['pings_per_minute']:
        return "3 pings per minute"
    elif 0.99 < row['pings_per_minute'] < 1.8:
        return "1 ping per minute"
    elif 1.7 < row['pings_per_minute'] < 2.8:
        return "2 pings per minute"
    else:
        return "No pings"
    
def final_df(vp_df, date: str):
    """
    Returns a final dataframe with all the requested metrics.
    A summarized version of the df is saved into 
    the RT_SCHED_GCS folder and a full one is returned for graphing.
    """
    df = merge_schedule_vp(vp_df, date).drop(columns = ['_merge'])
    
    # Find RT trip time versus scheduled trip time.
    # Find pings per minute.
    df = df.assign(
        rt_data_proportion_percentage = ((df.total_rt_min_coverage/df.scheduled_service_minutes)*100).fillna(0),
        pings_per_minute = (df.trip_ping_count/df.total_rt_min_coverage).fillna(0))
    
    # Any proportion above 100, mask as 100
    df["rt_data_proportion_percentage"] = df["rt_data_proportion_percentage"].mask(df["rt_data_proportion_percentage"] > 100, 100)
  
    # Fill in NA 
    df = df.fillna(
    df.dtypes.replace({"float64": 0.0, "object": "None"}))
    
    # Save to parquet
    # Drop some columns before saving to a parquet
    cols_to_keep = ['_gtfs_dataset_name', 'gtfs_dataset_key', 
                    'feed_key','trip_id', 'rt_data_proportion_percentage',
                    'rt_trip_counts_by_operator', 'trip_ping_count', 'pings_per_minute',]
    
    df[cols_to_keep].to_parquet(f"{RT_SCHED_GCS}rt_vs_scheduled_metrics.parquet")
    
    # Add columns for making graphs
    # Round certain columns to eyeball
    for i in ['rt_data_proportion_percentage','actual_trip_duration_minutes']:
        df[f"rounded_{i}"] = (((df[i]/100)*10).astype(int)*10)
        # .astype(str) + '%'
        
    # Categorize actual trip duration
    df["trip_category"] = df.apply(trip_duration_categories, axis=1)
    
    # Categorize RT vs. scheduled coverage
    df["rt_category"] = df.apply(rt_data_proportion, axis=1)
    
    # Categorize pings
    df["ping_category"] = df.apply(pings_categories, axis=1)
    
    # Clean
    df = threshold_utils.pre_clean(df)
    return df

"""
Summary Functions
"""
def rt_v_scheduled(df):
    """
    For every operator, find the # of trips 
    that fall into the RT vs. Scheduled % 
    bin and percentage of trips. 
    """
    df = (
    df.groupby(['Gtfs Dataset Name','Rounded Rt Data Proportion Percentage'])
    .agg({'Rt Trip Counts By Operator': "max", 'Trip Id': "nunique"})
    .reset_index()
    .rename(columns={'Trip Id': "Total Trips"}))
    
    df["Percentage of Trips"] = (
    df['Total Trips'] / df['Rt Trip Counts By Operator'] * 100)
    
    df = df.round(1)
    
    return df

def rt_trip_duration(df):
    """
    For every operator, find
    the number of trips within each
    by RT vs. Scheduled % data captured 
    and trip duration bin.
    """
    df = (df.groupby(['Gtfs Dataset Name',  'Trip Category', 'Rt Category'])
    .agg({"Rt Trip Counts By Operator": "max", "Trip Id": "nunique"})
    .reset_index()
    .rename(columns={"Trip Id": "Total Trips"}))
    
    df["Total Trips"] = (df["Total Trips"]).astype(int)
    
    df["Percentage of Trips"] = (df["Total Trips"]).divide(
    df["Rt Trip Counts By Operator"]) * 100
    
    df = df.round(1)
    
    return df

def statewide_metrics(df):
    """
    Aggregate operator metrics up to 
    statewide.
    """
    # Get total trips for the day
    all_trips = df['Trip Id'].count()
    
    # % of trips by RT vs. Scheduled Proportion 
    rt_scheduled = rt_v_scheduled(df)
    
    # Trip Lengths and % of RT vs. Scheduled Time
    rt_trip = rt_trip_duration(df)
    
    # Group by for statewide metrics
    rt_scheduled_sw = (
    rt_scheduled.groupby(['Rounded Rt Data Proportion Percentage'])
    .agg({"Total Trips": "sum"})
    .reset_index())
    rt_scheduled_sw["Percentage of Trips"] = rt_scheduled_sw['Total Trips'].div(all_trips) * 100
    
    rt_trip_sw = (rt_trip.groupby(["Rt Category", "Trip Category"])
    .agg({"Total Trips": "sum"})
    .reset_index())
    rt_trip_sw["Percentage of Trips"] = rt_trip_sw['Total Trips'].div(all_trips) * 100
    
    return rt_scheduled_sw, rt_trip_sw

"""
Visuals
"""
def bar_chart(df, x_col:str, y_col:str, chart_title:str):
    chart = (alt.Chart(df)
    .mark_bar(size=40)
    .encode(
        x=alt.X(f"{x_col}:N",
            scale=alt.Scale(domain=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
        ),
        y=alt.Y(y_col, scale=alt.Scale(domain=[0, 100])),
        color=alt.Color(
            x_col,
            scale=alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS),
            legend=None,
        ),
        tooltip=df.columns.tolist(),
    )
    .properties(title=chart_title)
    .interactive())
    
    return chart

def stacked_bar_chart(df, x_col:str, y_col:str, color_col: str, chart_title:str):
    chart = (
    (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(x_col, axis=alt.Axis(labelAngle=-45)),
            y=alt.Y(y_col, scale=alt.Scale(domain=[0, 100])),
            color=alt.Color(
                color_col, scale=alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS)
            ),
            tooltip=df.columns.tolist(),
        )
        .properties(title=chart_title)
    )
    .interactive()
    )
        
    return chart

def operator_level_visuals(df):
    """
    Return 2 charts with visuals
    for operator level metrics.
    """
    # Get summarized df
    rt_scheduled = rt_v_scheduled(df)
    rt_trip = rt_trip_duration(df)
    
    # Create dropdown menu
    # Exclude "none" operators which are only scheduled data
    operator_wo_none = df.loc[df['Gtfs Dataset Name'] != "None"][['Gtfs Dataset Name']]
    dropdown_list = operator_wo_none['Gtfs Dataset Name'].unique().tolist()
    
    # Show only first operator by default
    initialize_first_op = sorted(dropdown_list)[0]
    input_dropdown = alt.binding_select(options=sorted(dropdown_list), name="Operator")
    
    selection = alt.selection_single(name="Operator",
    fields=['Gtfs Dataset Name'],
    bind=input_dropdown,
    init={'Gtfs Dataset Name': initialize_first_op})
    
    # Create charts 
    chart_scheduled = bar_chart(rt_scheduled, 'Rounded Rt Data Proportion Percentage', 'Percentage of Trips', "% of RT vs. Scheduled Minutes by Operator")
    chart_trip = stacked_bar_chart(rt_trip, 'Trip Category','Percentage of Trips', 'Rt Category', "Trip Lengths and % of RT vs. Scheduled Time by Operator")
    
    # Finalize charts
    chart_scheduled = threshold_utils.chart_size(chart_scheduled, 500, 400).add_selection(selection).transform_filter(selection)
    chart_trip = threshold_utils.chart_size(chart_trip, 500, 400).add_selection(selection).transform_filter(selection)
    
    return chart_scheduled | chart_trip

def create_statewide_visuals(df):
    """
    Return 2 charts with visuals
    for statewide level metrics.
    """
    sw_scheduled, sw_trips = statewide_metrics(df)
    
    # Create charts
    chart_scheduled = bar_chart(sw_scheduled, 'Rounded Rt Data Proportion Percentage', 'Percentage of Trips', "% of RT vs. Scheduled Minutes for All Operators")
    chart_trip = stacked_bar_chart(sw_trips, 'Trip Category','Percentage of Trips', 'Rt Category', "Trip Lengths and % of RT vs. Scheduled Time for All Operators")
    
    # Finalize charts
    chart_scheduled = threshold_utils.chart_size(chart_scheduled, 500, 400)
    chart_trip = threshold_utils.chart_size(chart_trip, 500, 400)
    
    return chart_scheduled | chart_trip