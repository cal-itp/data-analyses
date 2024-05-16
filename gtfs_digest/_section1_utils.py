# Regular
import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd
from calitp_data_analysis.sql import to_snakecase
    
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

# Datetime
#import calendar
#from datetime import datetime
import numpy as np
from segment_speed_utils import helpers, time_series_utils

# Warehouse
import os
from calitp_data_analysis.sql import query_sql
from calitp_data_analysis.tables import tbls
from siuba import *
"""
Data
"""
def organization_name_crosswalk(organization_name: str) -> str:
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    og = pd.read_parquet(
        schd_vp_url, filters=[[("organization_name", "==", organization_name)]],
    )
    
    og = og.sort_values(by = ['service_date'], ascending = False)
    name = og.name.values[0]
    return name

def load_operator_profiles(organization_name:str)->pd.DataFrame:

    op_profiles_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_profiles}.parquet"

    op_profiles_df = pd.read_parquet(
    op_profiles_url,
    filters=[[("organization_name", "==", organization_name)]])
    
    op_profiles_df1 = op_profiles_df.sort_values(by = ['service_date'], ascending = False).head(1)
    
    # Rename dataframe
    op_profiles_df1.columns = op_profiles_df1.columns.map(_report_utils.replace_column_names)
    return op_profiles_df1 

def load_operator_map(name:str)->gpd.GeoDataFrame:
    op_routes_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_routes_map}.parquet"
    op_routes_gdf = gpd.read_parquet(
    op_routes_url,
    filters=[[("name", "==", name)]])
    
    # Grab max date
    max_date = op_routes_gdf.service_date.max()
    
    # Filter for only the most recent rows
    op_routes_gdf = op_routes_gdf.loc[op_routes_gdf.service_date == max_date]
    op_routes_gdf = op_routes_gdf.drop(columns = ['service_date'])
    
    # Rename dataframe
    op_routes_gdf.columns = op_routes_gdf.columns.map(_report_utils.replace_column_names)
    return op_routes_gdf

def load_scheduled_service(name:str)->pd.DataFrame:
    url = f"{GTFS_DATA_DICT.schedule_tables.dir}{GTFS_DATA_DICT.schedule_tables.monthly_scheduled_service}.parquet"
    df = pd.read_parquet(url,
    filters=[[("name", "==", name)]],)
    
    df["month"] = df["month"].astype(str).str.zfill(2)
    df["month"] = (df.year.astype(str)+ "-" + df.month.astype(str))
    df["datetime_date"] = pd.to_datetime(df["month"], format="%Y-%m")
    
    return df

def load_ntd(year: int) -> pd.DataFrame:
    """
    Load NTD Data stored in our warehouse.
    """
    df = (
        tbls.mart_ntd.dim_annual_ntd_agency_information()
        >> filter(_.year == year, _.state == "CA", _._is_current == True)
        >> select(
            _.number_of_state_counties,
            _.uza_name,
            _.density,
            _.number_of_counties_with_service,
            _.state_admin_funds_expended,
            _.service_area_sq_miles,
            _.population,
            _.service_area_pop,
            _.subrecipient_type,
            _.primary_uza,
            _.reporter_type,
            _.organization_type,
            _.agency_name,
            _.voms_pt,
            _.voms_do,
        )
        >> collect()
    )

    cols = list(df.columns)
    df2 = df.sort_values(by=cols, na_position="last")
    df3 = df2.groupby("agency_name").first().reset_index()

    return df3

def load_mobility()->pd.DataFrame:
    """
    Load mobility data in our warehouse.
    """
    df = (
    tbls.mart_transit_database.dim_mobility_mart_providers()
     >> select(
        _.agency_name,
        _.counties_served,
        _.hq_city,
        _.hq_county,
        _.is_public_entity,
        _.is_publicly_operating,
        _.funding_sources,
        _.on_demand_vehicles_at_max_service,
        _.vehicles_at_max_service
    )
    >> collect()
    )
    
    cols = list(df.columns)
    df2 = df.sort_values(by=cols, na_position='last')
    df2 = df2.sort_values(by=["on_demand_vehicles_at_max_service","vehicles_at_max_service"], ascending = [False, False])
    df3 = df2.groupby('agency_name').first().reset_index()
    return df3

def merge_ntd_mobility(year:int)->pd.DataFrame:
    ntd = load_ntd(year)
    mobility = load_mobility()
    m1 = pd.merge(
    mobility,
    ntd,
    how="inner",
    on="agency_name")
    agency_dict = {
    "City of Fairfield, California": "City of Fairfield",
    "Livermore / Amador Valley Transit Authority": "Livermore-Amador Valley Transit Authority",
    "Nevada County Transit Services": "Nevada County",
    "Omnitrans": "OmniTrans"}
    
    m1.agency_name = m1.agency_name.replace(agency_dict)
    m1.agency_name = m1.agency_name.str.strip()
    return m1

def ntd_operator_info(year:int, organization_name:str)->pd.DataFrame:
    ntd_mobility_df = merge_ntd_mobility(year)
    op_profiles = op_prep.operators_with_rt()[['organization_name']]
    op_profiles = op_profiles.loc[op_profiles.organization_name == organization_name].reset_index(drop = True)
    m1 = pd.merge(op_profiles, ntd_mobility_df,
                 how = "inner", left_on = ["organization_name"],
                 right_on = ["agency_name"])
    
    m1 = m1.fillna('None')
    return m1

"""
Data Manipulation
"""
def route_typology(df: pd.DataFrame)->pd.DataFrame:
    """
    Transform dataframe to display
    route types in a pie chart.
    """
    route_type_cols = [
       '# Downtown Local Route Types', '# Local Route Types',
       '# Rapid Route Types',"# Express Route Types",
         "# Rail Route Types"
    ]
    df = df[route_type_cols]
    df2 = df.T.reset_index()
    df2.columns = ['route_type','total_routes']
    return df2

def get_counties():
    ca_gdf = "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson"
    my_gdf = to_snakecase(gpd.read_file(f"{ca_gdf}"))[["county_name", "geometry"]]

    return my_gdf

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

    return df2

def total_service_hours_all_months(name: str) -> pd.DataFrame:
    """
    Find service hours for a full week for one operator
    and for the months we have a full week's worth of data downloaded.
    As of 5/2024, we have April 2023 and October 2023.
    """
    # Grab the dataframes with a full week's worth of data. 
    apr_week = rt_dates.get_week(month="apr2023", exclude_wed=False)
    oct_week = rt_dates.get_week(month="oct2023", exclude_wed=False)
    
    # Sum up total service_hours
    apr_df = total_service_hours(apr_week, name)
    oct_df = total_service_hours(oct_week, name)

    # Combine everything
    all_df = pd.concat([apr_df, oct_df])
    
    # Rename the columns
    all_df.columns = all_df.columns.map(_report_utils.replace_column_names)
    return all_df

"""
Charts & Maps
"""
def create_bg_service_chart():
    """
    Create a shaded background for the Service Hour Chart
    by Time Period. 
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

def create_service_hour_chart(df:pd.DataFrame, day_type:str):
    # Create an interactive legend
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
        y=alt.Y("Service Hours"),
        color=alt.Color(
            "Month",
            scale=alt.Scale(range=color_dict["tri_color"]),  # Specify desired order
        ),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
        tooltip=list(df.columns),
    )
    .properties(
        width=400,
        height=250,
        title={"text": title, 
               "subtitle": readable_dict["daily_scheduled_hour"]["subtitle"]},
    )
    .add_params(selection)
    )
    
    # display(main_chart)
    # Load background chart
    bg_chart = create_bg_service_chart()
    # display(bg_chart)
    
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
                    range=color_dict["longest_shortest_route"],
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

def basic_pie_chart(df: pd.DataFrame, color_col: str, theta_col: str, title: str,
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
    
def interactive_map(gdf:gpd.GeoDataFrame):
    gdf = gdf[~gdf.geometry.is_empty].reset_index(drop = True)
    gdf = gdf[gdf.geometry.notna()].reset_index(drop = True)
    def plot_route(route):
        filtered_gdf = gdf[gdf["Route"] == route]
        display(filtered_gdf.explore(column="Route", cmap = "Spectral",
        tiles="CartoDB positron",
        width=400,
        height=250,
        style_kwds={"weight": 3},
        legend=False,
        tooltip=["Route", "Service Miles"]))
    
    routes = gdf["Route"].unique().tolist()
    # Create a dropdown widget
    route_dropdown = widgets.Dropdown(options=routes, description="Route")
    # Create an interactive plot
    interactive_plot = widgets.interactive(plot_route, route=route_dropdown)
    display(widgets.VBox([interactive_plot])) 