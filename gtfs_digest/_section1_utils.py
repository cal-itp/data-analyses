import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

# Charts & Maps
from calitp_data_analysis import calitp_color_palette as cp
import altair as alt
import ipywidgets as widgets

# Display
from IPython.display import HTML, Markdown, display

# Other
from segment_speed_utils.project_vars import RT_SCHED_GCS, SCHED_GCS
from shared_utils import catalog_utils, rt_dates, rt_utils
import _report_utils 
import calendar
from datetime import datetime
import numpy as np

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

# Readable Dictionary
import yaml
with open("readable.yml") as f:
    readable_dict = yaml.safe_load(f)
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
    url = f"{GTFS_DATA_DICT.schedule_tables.gcs_dir}{GTFS_DATA_DICT.schedule_tables.monthly_scheduled_service}.parquet"
    df = pd.read_parquet(url,
    filters=[[("name", "==", name)]],)
    
    df["month"] = df["month"].astype(str).str.zfill(2)
    df["full_date"] = (df.year.astype(str)+ "-" + df.month.astype(str))
    df["datetime_date"] = pd.to_datetime(df["full_date"], format="%Y-%m")
    
    return df

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

def counties_served(gdf:gpd.GeoDataFrame)->pd.DataFrame:
    """
    Find which counties an operator serves.
    """
    ca_counties = get_counties()
    counties_served = gpd.sjoin(
    gdf,
    ca_counties.to_crs(gdf.crs),
    how="inner",
    predicate="intersects",
).drop(columns="index_right")
    
    counties_served = counties_served[["county_name"]].drop_duplicates().reset_index(drop = True)
    counties_served['county_name'] = "-" + counties_served['county_name']
    display(
        counties_served.style.hide(axis="index")
        .hide(axis="columns")
        .set_properties(**{"background-color": "white"})
        .set_table_styles([{"selector": "td, th", "props": [("text-align", "center")]}])
    )

def shortest_longest_route(gdf:gpd.GeoDataFrame)->pd.DataFrame:
    df = (
    gdf[["Route", "Service Miles"]]
    .sort_values(by=["Service Miles"])
    .iloc[[0, -1]])
    
    return df

def summarize_monthly(df:pd.DataFrame)->pd.DataFrame:
    df2 = (
    df.groupby(
        ['name', 'full_date','time_of_day', 'day_name']
    )
    .agg(
        {
            "ttl_service_hours": "sum",
        }
    )
    .reset_index()
    )
    
    return df2

def convert_to_timestamps(datetime_list):
    timestamps = []
    for dt in datetime_list:
        timestamp = dt.astype("datetime64[s]").astype(datetime)
        timestamps.append(timestamp)
    return timestamps

def count_days_in_months(dates: list) -> pd.DataFrame:
    # Turn list from numpy datetime to timestamp
    dates2 = convert_to_timestamps(dates)
    # Initialize a dictionary to store counts for each day of the week
    day_counts = {}

    # Iterate over each date
    for date in dates2:
        year = date.year
        month = date.month

        # Initialize counts dictionary for the current month-year combination
        if (year, month) not in day_counts:
            day_counts[(year, month)] = {
                "Monday": 0,
                "Tuesday": 0,
                "Wednesday": 0,
                "Thursday": 0,
                "Friday": 0,
                "Saturday": 0,
                "Sunday": 0,
            }

        # Get the calendar matrix for the current month and year
        matrix = calendar.monthcalendar(year, month)

        # Iterate over each day in the matrix
        for week in matrix:
            for i, day in enumerate(week):
                # Increment the count for the corresponding day of the week
                if day != 0:
                    weekday = calendar.day_name[i]
                    day_counts[(year, month)][weekday] += 1

    # Convert the dictionary to a pandas DataFrame
    df = pd.DataFrame.from_dict(day_counts, orient="index")
    df = df.reset_index()
    df["level_1"] = df["level_1"].astype(str).str.zfill(2)
    df["full_date"] = df.level_0.astype(str) + "-" + df.level_1.astype(str)
    df = df.drop(columns=["level_0", "level_1"])
    
    # Melt from wide to long
    df2 = pd.melt(
    df,
    id_vars=["full_date"],
    value_vars=[
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
        "full_date",
    ])
    
    df2 = df2.rename(columns = {"variable":"day_name", "value":"n_days"})
    return df2

def total_monthly_service(name:str) ->pd.DataFrame:
    
    df = load_scheduled_service(name)
    
    # Grab unique dates
    unique_dates = list(df.datetime_date.unique())
    
    # Find number of Monday's, Tuesday's...etc in each date
    month_days_df = count_days_in_months(unique_dates)
    
    # Aggregate the original dataframe
    agg_df = summarize_monthly(df)
    
    # Merge on number of day types
    agg_df = pd.merge(agg_df, month_days_df, on =["full_date", "day_name"], how = "left")
    
    # Find daily service hours
    agg_df["Daily Service Hours"] = agg_df.ttl_service_hours / agg_df.n_days
    
    # Rename columns
    agg_df.columns = agg_df.columns.map(_report_utils.replace_column_names)
    
    return agg_df

"""
Charts & Maps
"""
def single_bar_chart_dropdown(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    offset_col: str,
    title: str,
    dropdown_col: str,
    subtitle:str
):
    dropdown_list = df[dropdown_col].unique().tolist()
    dropdown_list.sort(reverse=True)
    dropdown = alt.binding_select(options=dropdown_list, name=_report_utils.labeling(dropdown_col))

    selector = alt.selection_point(
        name=_report_utils.labeling(dropdown_col), fields=[dropdown_col], bind=dropdown
    )

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{x_col}:N",
                title="Day",
                scale=alt.Scale(
                    domain=[
                        "Monday",
                        "Tuesday",
                        "Wednesday",
                        "Thursday",
                        "Friday",
                        "Saturday",
                        "Sunday",
                    ]
                ),
            ),
            y=alt.Y(f"{y_col}:Q", title=_report_utils.labeling(y_col)),
            xOffset=f"{offset_col}:N",
            color=alt.Color(
                f"{offset_col}:N",
                title=_report_utils.labeling(offset_col),
                scale=alt.Scale(
                    range=_report_utils.service_hour_scale,
                ),
            ),
            tooltip=df.columns.tolist(),
        )
    )
    chart = chart.properties(
        title = {
                "text": [title],
                "subtitle": [subtitle],
            }, width=500, height=300)
    chart = chart.add_params(selector).transform_filter(selector)

    display(chart)
    
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
                    range=_report_utils.section1,
                )
        ),
        tooltip = [x_col, y_col])
        .properties(
            width=500,
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
                scale=alt.Scale(range=_report_utils.service_hour_scale)
            ),
            tooltip=df.columns.tolist(),
        ).properties(
            width=250,
            height=300,
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
    width=500,
    height=300,
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
        width=500,
        height=300,
        style_kwds={"weight": 3},
        legend=False,
        tooltip=["Route", "Service Miles"]))
    
    routes = gdf["Route"].unique().tolist()
    # Create a dropdown widget
    route_dropdown = widgets.Dropdown(options=routes, description="Route")
    # Create an interactive plot
    interactive_plot = widgets.interactive(plot_route, route=route_dropdown)
    display(widgets.VBox([interactive_plot])) 