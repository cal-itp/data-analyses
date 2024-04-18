import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

# Charts
from calitp_data_analysis import calitp_color_palette as cp
import altair as alt

# Display
from IPython.display import HTML, Markdown, display

# Other
from segment_speed_utils.project_vars import RT_SCHED_GCS, SCHED_GCS
from shared_utils import catalog_utils, rt_dates, rt_utils
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

import _report_utils

"""
Datasets
"""
def load_operator_profiles(organization_name:str)->pd.DataFrame:
    op_profiles_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_profiles}.parquet"
    op_profiles_df = pd.read_parquet(
    op_profiles_url,
    filters=[[("organization_name", "==", organization_name)]])
    
    op_profiles_df1 = op_profiles_df.sort_values(by = ['service_date'], ascending = False).head(1)
    return op_profiles_df1 

def load_operator_map(name:str)->gpd.GeoDataFrame:
    op_routes_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_routes_map}.parquet"
    op_routes_gdf = gpd.read_parquet(
    op_routes_url,
    filters=[[("name", "==", name)]])
    
    op_routes_gdf = op_routes_gdf.sort_values(by = ['service_date'], ascending = False)
    return op_routes_gdf

def organization_name_crosswalk(organization_name: str) -> str:
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    og = pd.read_parquet(
        schd_vp_url, filters=[[("organization_name", "==", organization_name)]]
    )

    name = og.name.values[0]
    return name

def tag_day(df: pd.DataFrame, col_to_change:str) -> pd.DataFrame:
    # Function to determine if a date is a weekend day or a weekday
    def which_day(date):
        if date == 1:
            return "Monday"
        elif date == 2:
            return "Tuesday"
        elif date == 3:
            return "Wednesday"
        elif date == 4:
            return "Thursday"
        elif date == 5:
            return "Friday"
        elif date == 6:
            return "Saturday"
        else:
            return "Sunday"

    # Apply the function to each value in the "service_date" column
    df[col_to_change] = df[col_to_change].apply(which_day)

    return df

def load_scheduled_service(year:str, name:str)->pd.DataFrame:
    url = f"{GTFS_DATA_DICT.schedule_tables.gcs_dir}{GTFS_DATA_DICT.schedule_tables.monthly_scheduled_service}_{year}.parquet"
    df = pd.read_parquet(url,
    filters=[[("name", "==", name)]],)
    
    df["month"] = df["month"].astype(str).str.zfill(2)
    df["full_date"] = (df.year.astype(str)+ "-" + df.month.astype(str))
    df = tag_day(df, "day_type")
    
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
        "operator_n_routes",
        "n_downtown_local_routes",
        "n_rapid_routes",
        "n_local_routes",
    ]
    df = df[route_type_cols]
    df2 = df.T.reset_index()
    df2.columns = ['route_type','total_routes']
    df3 = df2.loc[df2.route_type != "operator_n_routes"]
    return df3

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
    
    counties_served = counties_served[["county_name"]].drop_duplicates()
    display(counties_served.style.hide().hide(axis="columns"))

def summarize_monthly(name:str)->pd.DataFrame:
    df_2023 = load_scheduled_service("2023", name)
    df_2024 = load_scheduled_service("2024", name)
    
    df = pd.concat([df_2023, df_2024])
    df2 = (
    df.groupby(
        ['name', 'full_date','time_of_day', 'day_type']
    )
    .agg(
        {
            "ttl_service_hours": "max",
        }
    )
    .reset_index()
    )
    return df2

def shortest_longest_route(gdf:gpd.GeoDataFrame)->pd.DataFrame:
    df = (
    gdf[["route_combined_name", "route_length_miles"]]
    .sort_values(by=["route_length_miles"])
    .iloc[[0, -1]])
    
    return df
"""
Charts
"""
def single_bar_chart_dropdown(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    offset_col: str,
    title: str,
    dropdown_col: str,
):
    dropdown_list = sorted(df[dropdown_col].unique().tolist())

    dropdown = alt.binding_select(options=dropdown_list, name=_report_utils.labeling(dropdown_col))

    selector = alt.selection_single(
        name=_report_utils.labeling(dropdown_col), fields=[dropdown_col], bind=dropdown
    )

    ruler = (
        alt.Chart(df)
        .mark_rule(color="red", strokeDash=[10, 7])
        .encode(y=f"mean({y_col}):Q")
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
                scale=alt.Scale(
                    range=cp.CALITP_SEQUENTIAL_COLORS,
                ),
            ),
            tooltip=df.columns.tolist(),
        )
    )
    chart = (chart + ruler).properties(title=title, width=500, height=300)
    chart = chart.add_params(selector).transform_filter(selector)

    display(chart)
    
def basic_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, title: str):
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(x_col, title=_report_utils.labeling(x_col)),
            y=alt.Y(y_col, title=_report_utils.labeling(y_col)),
            color=alt.Color(
               y_col,
                scale=alt.Scale(
                    range=cp.CALITP_SEQUENTIAL_COLORS,
                ),
        ))
        .properties(
            width=500,
            title={
                "text": title,
            },
        )
    )
    return chart

def basic_pie_chart(df: pd.DataFrame, color_col: str, theta_col: str, title: str):
    chart = (
        alt.Chart(df)
        .mark_arc()
        .encode(
            theta=theta_col,
            color=alt.Color(
                color_col, scale=alt.Scale(range=_report_utils.blue_palette)
            ),
            tooltip=df.columns.tolist(),
        ).properties(
            width=250,
            height=300,
            title={
                "text": title,
            },
        ))
    

    return chart

def display_all_routes(gdf:gpd.GeoDataFrame):
    display(gdf.drop(columns=["service_date"]).explore(
    "route_combined_name",
    tiles="CartoDB positron",
    width=500,
    height=300,
    style_kwds={"weight": 3},
    legend=False,
    tooltip=["route_combined_name", "route_length_miles"]))