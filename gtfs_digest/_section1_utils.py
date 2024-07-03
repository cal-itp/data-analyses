# Regular
import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd
from calitp_data_analysis.sql import to_snakecase
import numpy as np

# Project Specific Packages
from segment_speed_utils.project_vars import (COMPILED_CACHED_VIEWS, RT_SCHED_GCS, SCHED_GCS)
from shared_utils import catalog_utils, rt_dates, rt_utils
import _report_utils 
import _operators_prep as op_prep

# Readable Dictionary
import yaml
with open("readable.yml") as f:
    readable_dict = yaml.safe_load(f)
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

# Charts & Maps
from calitp_data_analysis import calitp_color_palette as cp
import altair as alt
import ipywidgets
from IPython.display import HTML, Markdown, display
with open("color_palettes.yml") as f:
    color_dict = yaml.safe_load(f)

### Delete after incorporating stuff into pipeline
from segment_speed_utils import helpers, time_series_utils

import os
from calitp_data_analysis.sql import query_sql
from calitp_data_analysis.tables import tbls
from siuba import *

"""
Data
"""
def organization_name_crosswalk(organization_name: str) -> str:
    """
    Used to match organization_name field with name.
    """
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    og = pd.read_parquet(
        schd_vp_url, filters=[[("organization_name", "==", organization_name)]],
    )
    
    # Get most recent name
    og = og.sort_values(by = ['service_date'], ascending = False)
    name = og.name.values[0]
    return name

def load_operator_profiles(organization_name:str)->pd.DataFrame:
    """
    Load operator profile dataset for one operator
    """
    op_profiles_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_profiles}.parquet"

    op_profiles_df = pd.read_parquet(
    op_profiles_url,
    filters=[[("organization_name", "==", organization_name)]])
    
    # Keep only the most recent row
    op_profiles_df1 = op_profiles_df.sort_values(by = ['service_date'], ascending = False).head(1)
    
    ntd_cols = [
        "schedule_gtfs_dataset_key",
        "counties_served",
        "service_area_sq_miles",
        "hq_city",
        "uza_name",
        "service_area_pop",
        "organization_type",
        "primary_uza",
        "reporter_type"
    ]
    
    # Load NTD through the crosswalk for the most recent date
    most_recent_date = rt_dates.y2024_dates[-1]
    
    ntd_df = helpers.import_schedule_gtfs_key_organization_crosswalk(most_recent_date)[
    ntd_cols]
    
    # Try to merge
    op_profiles_df1 = pd.merge(op_profiles_df1, ntd_df, on = ["schedule_gtfs_dataset_key"], how = "left")
        
    # Rename dataframe
    op_profiles_df1.columns = op_profiles_df1.columns.map(_report_utils.replace_column_names)
    
    return op_profiles_df1 

def load_operator_map(name:str)->gpd.GeoDataFrame:
    """
    Load geodataframe with all of the operator's routes.
    """
    op_routes_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_routes_map}.parquet"
    op_routes_gdf = gpd.read_parquet(
    op_routes_url,
    filters=[[("name", "==", name)]])
    
    # Find the most recent available geography for each route
    op_routes_gdf = op_routes_gdf.sort_values(by = ["service_date"], ascending = False)
    op_routes_gdf = op_routes_gdf.drop_duplicates(
    subset=["route_long_name", "route_short_name", "route_combined_name"]
    )
    op_routes_gdf = op_routes_gdf.drop(columns = ['service_date'])
    
    # Rename dataframe
    op_routes_gdf.columns = op_routes_gdf.columns.map(_report_utils.replace_column_names)
    return op_routes_gdf

def get_counties():
    ca_gdf = "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson"
    my_gdf = to_snakecase(gpd.read_file(f"{ca_gdf}"))[["county_name", "geometry"]]

    return my_gdf
"""
Data Manipulation
Change dataframes from long to wide
"""
def route_typology(df: pd.DataFrame)->pd.DataFrame:
    """
    Transform dataframe to display
    route types in a pie chart.
    """
    route_type_cols = [
       '# Downtown Local Route Types',
       '# Local Route Types', '# Coverage Route Types', '# Rapid Route Types',
       '# Express Route Types', '# Rail Route Types'
    ]
    df = df[route_type_cols]
    df2 = df.T.reset_index()
    df2.columns = ['route_type','total_routes']
    return df2

def concat_all_columns(df):
    # Concatenate all columns into a new column called 'all'
    df['all'] = df.apply(lambda row: ', '.join(row.astype(str)), axis=1)

    return df

def counties_served(gdf:gpd.GeoDataFrame)->pd.DataFrame:
    """
    Find which counties an operator serves.
    """
    ca_counties = get_counties()
    counties_served = gpd.sjoin(
    gdf,
    ca_counties.to_crs(gdf.crs),
    how="inner",
    predicate="intersects").drop(columns="index_right")
    
    counties_served = (counties_served[["county_name"]]
                       .drop_duplicates()
                       .sort_values(by = ["county_name"])
                       .reset_index(drop = True)
                      )
    counties_served = counties_served.T
    counties_served = concat_all_columns(counties_served)
    return counties_served

def shortest_longest_route(gdf:gpd.GeoDataFrame)->pd.DataFrame:
    df = (
    gdf[["Route", "Service Miles"]]
    .sort_values(by=["Service Miles"])
    .iloc[[0, -1]])
    
    return df

"""
Charts & Maps
"""
def create_bg_service_chart():
    """
    Create a shaded background for the Service Hour Chart
    by time period. 
    """
    cutoff = pd.DataFrame(
    {
        "start": [0, 4, 7, 10, 15, 19],
        "stop": [3.99, 6.99, 9.99, 14.99, 18.99, 24],
        "time_period": [
            "Owl:12-3:59AM",
            "Early AM:4-6:59AM",
            "AM Peak:7-9:59AM",
            "Midday:10AM-2:59PM",
            "PM Peak:3-7:59PM",
            "Evening:8-11:59PM",
        ],
    }
    )
    
    # Sort legend by time, 12am starting first. 
    chart = alt.Chart(cutoff.reset_index()).mark_rect(opacity=0.15).encode(
    x="start",
    x2="stop",
    y=alt.value(0),  # pixels from top
    y2=alt.value(250),  # pixels from top
    color=alt.Color(
        "time_period:N",
        sort = (
            [
                "Owl:12-3:59AM",
                "Early AM:4-6:59AM",
                "AM Peak:7-9:59AM",
                "Midday:10AM-2:59PM",
                "PM Peak:3-7:59PM",
                "Evening:8-11:59PM",
            ]
        ),
        title=_report_utils.labeling("time_period"),
        scale=alt.Scale(range=color_dict["full_color_scale"]),
    ))
    
    return chart

def create_service_hour_chart(df:pd.DataFrame,
                              day_type:str,
                              y_col:str,
                              subtitle:str):
    # Create an interactive legend so you can view
    # lines by year 
    selection = alt.selection_point(fields=['Month'], bind='legend')
    
    # Create the main line chart
    df = df.loc[df["Weekend or Weekday"] == day_type].reset_index(drop = True)
    
    # Create a new title that incorporates day type
    title = readable_dict["daily_scheduled_hour"]["title"]
    title = title + ' for ' + day_type

    main_chart = (
    alt.Chart(df)
    .mark_line(size=3)
    .encode(
        x=alt.X("Departure Hour", 
                title=_report_utils.labeling("Departure Hour in Military Time")),
        y=alt.Y(y_col,
               title = _report_utils.labeling(y_col)),
        color=alt.Color(
            "Month",
            scale=alt.Scale(range=color_dict["four_color"]),  # Specify desired order
        ),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
        tooltip=list(df.columns),
    )
    .properties(
        width=400,
        height=250,
        title={"text": title, 
               "subtitle": subtitle},
    )
    .add_params(selection)
    )
    
    # Load background chart
    bg_chart = create_bg_service_chart()
    
    # Combine
    final_chart = (main_chart + bg_chart).properties(
    resolve=alt.Resolve(
        scale=alt.LegendResolveMap(color=alt.ResolveMode("independent"))
    )
    )
    
    return final_chart

def basic_bar_chart(df: pd.DataFrame, x_col: str, y_col: str,
                    title: str, subtitle:str):
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(x_col, title=_report_utils.labeling(x_col)),
            y=alt.Y(y_col, title=_report_utils.labeling(y_col)),
            color=alt.Color(
               y_col,
                legend = None,
                title=_report_utils.labeling(y_col),
                scale=alt.Scale(
                    range=color_dict["tri_color"],
                )
        ),
        tooltip = [x_col, y_col])
        .properties(
            width=400,
            title={
                "text": [title],
                "subtitle": [subtitle],
            }
        )
    )
    return chart

def basic_pie_chart(df: pd.DataFrame, 
                    color_col: str, 
                    theta_col: str, 
                    title: str,
                    subtitle:str):
    chart = (
        alt.Chart(df)
        .mark_arc()
        .encode(
            theta=theta_col,
            color=alt.Color(
                color_col, 
                title=_report_utils.labeling(color_col),
                scale=alt.Scale(range=color_dict["full_color_scale"])
            ),
            tooltip=df.columns.tolist(),
        ).properties(
            width=400,
            height=250,
            title={
                "text": [title],
                "subtitle": [subtitle],
            }
        ))
    
    return chart

"""
Maps
"""
def plot_route(route):
    filtered_gdf = gdf[gdf["Route"] == route]
    display(filtered_gdf.explore(column="Route", cmap = "Spectral",
    tiles="CartoDB positron",
    width=400,
    height=250,
    style_kwds={"weight": 3},
    legend=False,
    tooltip=["Route", "Service Miles"]))
    
"""
Stuff to delete after incorporating into the pipeline 
"""
def ntd_operator_info(year:int, organization_name:str)->pd.DataFrame:
    ntd_mobility_df = merge_ntd_mobility(year)
    op_profiles = op_prep.operators_with_rt()[['organization_name']]
    op_profiles = op_profiles.loc[op_profiles.organization_name == organization_name].reset_index(drop = True)
    m1 = pd.merge(op_profiles, ntd_mobility_df,
                 how = "inner", left_on = ["organization_name"],
                 right_on = ["agency_name"])
    
    m1 = m1.fillna('None')
    return m1

def concatenate_trips(
    date_list: list,
) -> pd.DataFrame:
    """
    Concatenate schedule data that's been
    aggregated to route-direction-time_period for
    multiple days.
    """
    FILE = GTFS_DATA_DICT.schedule_downloads.trips

    df = (
        time_series_utils.concatenate_datasets_across_dates(
            COMPILED_CACHED_VIEWS,
            FILE,
            date_list,
            data_type="df",
            columns=[
                "name",
                "service_date",
                "route_long_name",
                "trip_first_departure_datetime_pacific",
                "service_hours",
            ],
        )
        .sort_values(["service_date"])
        .reset_index(drop=True)
    )

    return df

def get_day_type(date):
    """
    Function to return the day type (e.g., Monday, Tuesday, etc.) from a datetime object.
    """
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return days_of_week[date.weekday()]

def weekday_or_weekend(row):
    """
    Tag if a day is a weekday or Saturday/Sunday
    """
    if row.day_type == "Sunday":
        return "Sunday"
    if row.day_type == "Saturday":
        return "Saturday"
    else:
        return "Weekday"

def total_service_hours(date_list: list, name: str) -> pd.DataFrame:
    """
    Total up service hours by departure hour, 
    month, and day type for an operator. 
    """
    # Combine all the days' data for a week
    df = concatenate_trips(date_list)
    
     # Filter
    df = df.loc[df.name == name].reset_index(drop=True)
    
    # Add day type aka Monday, Tuesday, Wednesday...
    df['day_type'] = df['service_date'].apply(get_day_type)
    
    # Tag if the day is a weekday, Saturday, or Sunday
    df["weekend_weekday"] = df.apply(weekday_or_weekend, axis=1)
    
    # Find the minimum departure hour
    df["departure_hour"] = df.trip_first_departure_datetime_pacific.dt.hour
    
    # Delete out the specific day, leave only month & year
    df["month"] = df.service_date.astype(str).str.slice(stop=7)
    
    df2 = (
        df.groupby(["name", "month", "weekend_weekday", "departure_hour"])
        .agg(
            {
                "service_hours": "sum",
            }
        )
        .reset_index()
    )
    df2["weekday_service_hours"] = df2.service_hours/5
    df2 = df2.rename(columns = {'service_hours':'weekend_service_hours'})
    return df2

def total_service_hours_all_months(name: str) -> pd.DataFrame:
    """
    Find service hours for a full week for one operator
    and for the months we have a full week's worth of data downloaded.
    As of 5/2024, we have April 2023 and October 2023.
    """
    # Grab the dataframes with a full week's worth of data. 
    apr_23week = rt_dates.get_week(month="apr2023", exclude_wed=False)
    oct_23week = rt_dates.get_week(month="oct2023", exclude_wed=False)
    apr_24week = rt_dates.get_week(month="apr2024", exclude_wed=False)
    # need to add april 2024 here 
    
    # Sum up total service_hours
    apr_23df = total_service_hours(apr_23week, name)
    oct_23df = total_service_hours(oct_23week, name)
    apr_24df = total_service_hours(apr_24week, name)
    
    # Combine everything
    all_df = pd.concat([apr_23df, oct_23df, apr_24df])
    
    # Rename the columns
    all_df.columns = all_df.columns.map(_report_utils.replace_column_names)
    return all_df

