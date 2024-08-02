##import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd
import datetime

# Charts
import altair as alt
alt.data_transformers.enable('default', max_rows=None)
import _report_utils

# Display
from IPython.display import HTML, Markdown, display

# Other
from segment_speed_utils import gtfs_schedule_wrangling,helpers
from segment_speed_utils.project_vars import RT_SCHED_GCS, SCHED_GCS
from shared_utils import catalog_utils, rt_dates, rt_utils

# Data Dictionary
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
import yaml
with open("readable.yml") as f:
    readable_dict = yaml.safe_load(f)

# Color Palette 
with open("color_palettes.yml") as f:
    color_dict = yaml.safe_load(f)
    
"""
Data
"""
def load_schedule_vp_metrics(organization:str)->pd.DataFrame:
    """
    Load schedule versus realtime file.
    """
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
    # Keep only rows that are found in both schedule and real time data
    df = (pd.read_parquet(schd_vp_url, 
          filters=[[("organization_name", "==", organization),
         ("sched_rt_category", "==", "schedule_and_vp")]])
         )
    
    # Delete duplicates
    df = df.drop_duplicates().reset_index(drop = True)
    
    # Round float columns
    float_columns = df.select_dtypes(include=['float'])
    for i in float_columns:
        df[i] = df[i].round(2)
    
    # Multiply percent columns to 100% 
    pct_cols = df.columns[df.columns.str.contains("pct")].tolist()
    for i in pct_cols:
        df[i] = df[i] * 100
        
    # Add column to create rulers for the charts
    df["ruler_100_pct"] = 100
    df["ruler_for_vp_per_min"] = 2
    
    # Add a column that flips frequency to be every X minutes instead
    # of every hour.
    df["frequency_in_minutes"] = 60/df.frequency
    
    # Replace column names
    df.columns = df.columns.map(_report_utils.replace_column_names)

    return df

"""
Data Manipulation
"""
def route_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find overall statistics for a route.
    This dataframe backs the last two text table charts.
    """
    most_recent_date = df["Date"].max()
    route_merge_cols = ["Route", "Direction", "dir_0_1"]
    
    # Filter out for the most recent date. 
    # Create 3 separate dataframes for all day, peak, and offpeak.
    all_day_stats = df[(df["Date"] == most_recent_date) & (df["Period"] == "all_day")][
        route_merge_cols
        + [
            "Average Scheduled Service (trip minutes)",
            "Average Stop Distance (miles)",
            "# scheduled trips",
            "GTFS Availability",
        ]
    ]

    peak_stats = df[(df["Date"] == most_recent_date) & (df["Period"] == "peak")][
        route_merge_cols + ["Speed (MPH)", "# scheduled trips", "Trips per Hour"]
    ].rename(
        columns={
            "Speed (MPH)": "peak_avg_speed",
            "# scheduled trips": "peak_scheduled_trips",
            "Trips per Hour": "peak_hourly_freq",
        }
    )

    offpeak_stats = df[(df["Date"] == most_recent_date) & (df["Period"] == "offpeak")][
        route_merge_cols + ["Speed (MPH)", "# scheduled trips", "Trips per Hour"]
    ].rename(
        columns={
            "Speed (MPH)": "offpeak_avg_speed",
            "# scheduled trips": "offpeak_scheduled_trips",
            "frequency": "offpeak_hourly_freq",
        }
    )

    table_df = (
        pd.merge(all_day_stats, peak_stats, on=route_merge_cols, how="outer")
        .merge(offpeak_stats, on=route_merge_cols, how="outer")
        .sort_values(["Route", "Direction"])
        .reset_index(drop=True)
    )
    
    # Fill nans
    numeric_cols = table_df.select_dtypes(include="number").columns
    table_df[numeric_cols] = table_df[numeric_cols].fillna(0)
    
    # Clean up column names
    table_df.columns = table_df.columns.str.title().str.replace("_", " ")
    return table_df

def timeliness_trips(df: pd.DataFrame):
    """
    Reshape dataframe for the charts that illustrate
    how timely a route's trips are. 
    """
    to_keep = [
        "Date",
        "Organization",
        "Direction",
        "Period",
        "Route",
        "# Early Arrival Trips",
        "# On-Time Trips",
        "# Late Trips",
        "dir_0_1",
    ]
    
    # Filter out any values that don't equal to all day. 
    # Keeping all day values will double count the trips.
    df = df.loc[(df["Period"] != "All Day")]
    df = df.loc[df["Period"] != "all_day"]
    df2 = df[to_keep]

    melted_df = df2.melt(
        id_vars=[
            "Date",
            "Organization",
            "Route",
            "Period",
            "Direction",
            "dir_0_1",
        ],
        value_vars=[
            "# Early Arrival Trips",
            "# On-Time Trips",
            "# Late Trips",
        ],
    )
    return melted_df

def pct_vp_journey(df: pd.DataFrame, col1: str, col2: str) -> pd.DataFrame:
    """
    Reshape the data for the charts that display the % of 
    a journey that recorded 2+ vehicle positions/minute.
    """
    to_keep = [
        "Date",
        "Organization",
        "Direction",
        col1,
        col2,
        "Route",
        "Period",
        "ruler_100_pct",
    ]
    df2 = df[to_keep]

    df3 = df2.melt(
        id_vars=[
            "Date",
            "Organization",
            "Route",
            "Direction",
            "Period",
            "ruler_100_pct",
        ],
        value_vars=[col1, col2],
    )

    df3 = df3.rename(
        columns={"variable": "Category", "value": "% of Actual Trip Minutes"}
    )
    return df3

"""
Charts
"""
def divider_chart(df: pd.DataFrame, text):
    """
    This chart creates only one line of text.
    I use this to divide charts thematically.
    """
    df = df.head(1)
    # Create a text chart using Altair
    chart = (
        alt.Chart(df)
        .mark_text(
            align="center",
            baseline="middle",
            fontSize=14,
            fontWeight="bold",
            text=text,
        )
        .properties(width=400, height=100)
    )

    return chart

def clean_data_charts(df:pd.DataFrame, y_col:str)->pd.DataFrame:
    """
    Do some basic cleaning to the datafarmes.
    """
    df["Period"] = df["Period"].str.replace("_", " ").str.title()
    df[y_col] = df[y_col].fillna(0).astype(int)
    df[f"{y_col}_str"] = df[y_col].astype(str)
    
    return df

def set_y_axis(df, y_col):
    """
    Set y_axis automatically depending on the
    column used to generate the y_axis.
    """
    if "%" in y_col:
        max_y = 100
    elif "VP" in y_col:
        max_y = 3
    elif "Minute" in y_col:
        max_y = round(df[y_col].max())
    else:
        max_y = round(df[y_col].max(), -1) + 5
    return max_y

def grouped_bar_chart(
    df: pd.DataFrame,
    color_col: str,
    y_col: str,
    offset_col: str,
    title: str,
    subtitle: str,
)-> alt.Chart:
    tooltip_cols = [
        "Period",
        "Route",
        "Date",
        "Direction",
        color_col,
        y_col,
    ]
    
    # Clean dataframe
    df = clean_data_charts(df, y_col)

    chart = (
        alt.Chart(df)
        .mark_bar(size=10)
        .encode(
            x=alt.X(
                "yearmonthdate(Date):O",
                title=["Grouped by Direction ID", "Date"],
                axis=alt.Axis(labelAngle=-45, format="%b %Y"),
            ),
            y=alt.Y(f"{y_col}:Q", title=_report_utils.labeling(y_col)),
            xOffset=alt.X(f"{offset_col}:N", title=_report_utils.labeling(offset_col)),
            color=alt.Color(
                f"{color_col}:N",
                title=_report_utils.labeling(color_col),
                scale=alt.Scale(range=color_dict["four_colors"]),
                ),
            tooltip=tooltip_cols,
        ))
    chart = (chart).properties(
        title={
            "text": [title],
            "subtitle": [subtitle],
        },
       width=400,
        height=250,
    )

    return chart
    
def base_facet_line(
    df: pd.DataFrame, 
    y_col: str, 
    title: str, 
    subtitle: str
) -> alt.Chart:
    
    # Set y-axis
    max_y = set_y_axis(df, y_col)
    
    # Clean dataframe
    df = clean_data_charts(df, y_col)
    
    tooltip_cols = [
            "Period",
            "Route",
            "Date",
            f"{y_col}_str",
            "Direction",
        ]

    chart = (
            alt.Chart(df)
            .mark_line(size=3)
            .encode(
                x=alt.X(
                    "yearmonthdate(Date):O",
                    title="Date",
                    axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                ),
                y=alt.Y(
                    f"{y_col}:Q",
                    title=_report_utils.labeling(y_col),
                    scale=alt.Scale(domain=[0, max_y]),
                ),
                color=alt.Color(
                    "Period:N",
                    title=_report_utils.labeling("Period"),
                    scale=alt.Scale(range=color_dict["tri_color"]),
                ),
                tooltip=tooltip_cols,
            )
        )

    chart = chart.properties(width=200, height=250)
    chart = chart.facet(
            column=alt.Column("Direction:N", title=_report_utils.labeling("Direction")),
        ).properties(
            title={
                "text": [title],
                "subtitle": [subtitle],
            }
        )
    return chart

def base_facet_circle(
    df: pd.DataFrame,
    y_col: str,
    color_col: str,
    ruler_col: str,
    title: str,
    subtitle: str,
) -> alt.Chart:

    tooltip_cols = [
        "Period",
        "Route",
        "Date",
        "Direction",
        f"{y_col}_str",
        color_col,
    ]
    
    # Set y-axis
    max_y = set_y_axis(df, y_col)
    
    # Clean dataframe
    df = clean_data_charts(df, y_col)
    
    # Create the ruler for the bar chart
    ruler = (
            alt.Chart(df)
            .mark_rule(color="red", strokeDash=[10, 7])
            .encode(y=f"ruler_100_pct:Q")
        )

    chart = (
            alt.Chart(df)
            .mark_circle(size=150)
            .encode(
                x=alt.X(
                    "yearmonthdate(Date):O",
                    title="Date",
                    axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                ),
                y=alt.Y(
                    f"{y_col}:Q",
                    title=_report_utils.labeling(y_col),
                    scale=alt.Scale(domain=[0, max_y]),
                ),
                color=alt.Color(
                    f"{color_col}:N",
                    title=_report_utils.labeling(color_col),
                    scale=alt.Scale(range=color_dict["tri_color"]),
                ),
                tooltip=tooltip_cols,
            )
        )
    # Add ruler plus main chart
    chart = (chart + ruler).properties(width=200, height=250)
    chart = chart.facet(
            column=alt.Column("Direction:N", title=_report_utils.labeling("Direction")),
        ).properties(
            title={
                "text": [title],
                "subtitle": [subtitle],
            }
        )
    return chart
    
def base_facet_chart(
    df: pd.DataFrame,
    direction_to_filter: int,
    y_col: str,
    color_col: str,
    facet_col: str,
    title: str,
    subtitle: str,
)-> alt.Chart:
    tooltip_cols = [
        "Period",
        "Route",
        "Date",
        "Direction",
        y_col,
        color_col,
    ]
    
    # Create a title.
    try:
        title = title + " for Direction " + str(direction_to_filter)
    except:
        pass
    
    # Set y-axis
    max_y = set_y_axis(df, y_col)
    
    # Clean dataframe
    df = clean_data_charts(df, y_col)

    chart = (
        alt.Chart(df)
        .mark_bar(size=7, clip=True)
        .encode(
            x=alt.X(
                "yearmonthdate(Date):O",
                title=["Date"],
                axis=alt.Axis(labelAngle=-45, format="%b %Y"),
            ),
            y=alt.Y(
                f"{y_col}:Q",
                title=_report_utils.labeling(y_col),
                scale=alt.Scale(domain=[0, max_y]),
            ),
            color=alt.Color(
                f"{color_col}:N",
                title=_report_utils.labeling(color_col),
                scale=alt.Scale(range=color_dict["tri_color"]),
            ),
            tooltip=tooltip_cols,
        )
    )
    chart = chart.properties(width=200, height=250)
    
    # Facet the chart
    chart = chart.facet(column=alt.Column(f"{facet_col}:N",)).properties(
        title={
            "text": title,
            "subtitle": subtitle,
        }
    )

    return chart
    
def base_facet_with_ruler_chart(
    df: pd.DataFrame,
    y_col: str,
    ruler_col: str,
    title: str,
    subtitle: str,
    domain_color:list,
    range_color:list,
) -> alt.Chart:
    tooltip_cols = [
        "Period",
        "Route",
        "Date",
        "Direction",
        y_col,
    ]
    
    # Set y-axis
    max_y = set_y_axis(df, y_col)
    
    # Clean dataframe
    df = clean_data_charts(df, y_col)
    
    # Create color scale
    color_scale = alt.Scale(
    domain= domain_color,
    range = range_color
    )
    
    # Create ruler
    ruler = (
            alt.Chart(df)
            .mark_rule(color="red", strokeDash=[10, 7])
            .encode(y=f"mean({ruler_col}):Q")
        )
    
    chart = (
            alt.Chart(df)
            .mark_bar(size=7, clip=True)
            .encode(
                x=alt.X(
                    "yearmonthdate(Date):O",
                    title=["Date"],
                    axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                ),
                y=alt.Y(
                    f"{y_col}:Q",
                    title=_report_utils.labeling(y_col),
                    scale=alt.Scale(domain=[0, max_y]),
                ),
                color=alt.Color(
                    f"{y_col}:Q",
                    title=_report_utils.labeling(y_col),
                    scale=color_scale,
                ),
                tooltip=df[tooltip_cols].columns.tolist(),
            )
        )

    chart = (chart + ruler).properties(width=200, height=250)
    chart = chart.facet(column=alt.Column("Direction:N",)).properties(
            title={
                "text": title,
                "subtitle": [subtitle],
            }
        )

    return chart

def create_text_table(df: pd.DataFrame, 
                      direction: int,
                      title:str,
                      subtitle:str)->alt.Chart:
    
    # Filter dataframe for direction
    df = df.loc[df["Dir 0 1"] == direction].drop_duplicates().reset_index(drop=True)
    
    # Reshape dataframe before plotting
    df2 = df.melt(
            id_vars=[
                "Route",
                "Direction",
            ],
            value_vars=[
                "Average Scheduled Service (Trip Minutes)",
                "Average Stop Distance (Miles)",
                "# Scheduled Trips",
                "Gtfs Availability",
                "Peak Avg Speed",
                "Peak Scheduled Trips",
                "Peak Hourly Freq",
                "Offpeak Avg Speed",
                "Offpeak Scheduled Trips",
                "Trips Per Hour",
            ],
        )
    # Create a decoy column so all the text will be centered.
    df2["Zero"] = 0
    
    # Combine columns so the column title and variable will be aligned.
    # Ex: "Trips Per Hour: 0.56". This column is what will show up on the
    # graphs.
    df2["combo_col"] = df2.variable.astype(str) + ": " + df2.value.astype(str)
    
    # Clean up 
    df2.combo_col = df2.combo_col.str.replace(
            "schedule_and_vp", "Schedule and Realtime Data",
        ).str.replace("Gtfs", "GTFS")
    
    # Create the charts
    text_chart = (
            alt.Chart(df2)
            .mark_text()
            .encode(x=alt.X("Zero:Q", axis=None),
                    y=alt.Y("combo_col", axis=None))
        )
    
    
    text_chart = (text_chart.encode(text="combo_col:N")
            .properties(
            title = 
                {"text":f"{title}{direction}",
                  "subtitle":subtitle},
            width=400,
            height=250,
        ))
    return text_chart

def frequency_chart(
    df: pd.DataFrame, 
    direction_id: int, 
    title: str, 
    subtitle: str
)->alt.Chart:
    
     # Filter the only one direction
    df = df.loc[df.dir_0_1 == direction_id].reset_index(drop=True)

    # Create a new column
    df["Frequency in Minutes"] = (
        "A trip going this direction comes every "
        + df.frequency_in_minutes.astype(int).astype(str)
        + " minutes"
    )
    
    # Clean the dataframe
    df = clean_data_charts(df, "frequency_in_minutes")
    
    # Create color scale
    color_scale = alt.Scale(
        domain=color_dict["freq_domain"], range=color_dict["freq_range"]
    )

    chart = (
        alt.Chart(df)
        .mark_bar(size=3, clip=True)
        .encode(
            y=alt.Y(
                "yearmonthdate(Date):O",
                title=["Date"],
                axis=alt.Axis(format="%b %Y"),
            ),
            x=alt.X(
                "frequency_in_minutes:Q",
                title=_report_utils.labeling("frequency_in_minutes"),
                scale=alt.Scale(domain=[0, 240]),
            ),
            color=alt.Color(
                "frequency_in_minutes:Q",
                scale=color_scale,
                title=_report_utils.labeling("frequency_in_minutes"),
            ),
            tooltip=["Date", "Route", "Frequency in Minutes", "Period", "Direction"],
        )
    )

    chart = chart.properties(width=120, height=100)

    title = title + " for Direction " + str(direction_id)
    chart = chart.facet(column=alt.Column("Period:N")).properties(
        title={
            "text": title,
            "subtitle": subtitle,
        }
    )
    return chart

"""
Route-Direction
Section
"""
def filtered_route(
    df: pd.DataFrame,
) -> alt.Chart:
    """
    This combines all the charts together, controlled by a single
    dropdown.
    
    Resources:
        https://stackoverflow.com/questions/58919888/multiple-selections-in-altair
    """
    # Create dropdown
    routes_list = df["Route"].unique().tolist()

    route_dropdown = alt.binding_select(
        options=routes_list,
        name="Routes: ",
    )
    # Column that controls the bar charts
    xcol_param = alt.selection_point(
    fields=["Route"], value=routes_list[0], bind=route_dropdown
    )

    # Filter for only rows that are "all day" statistics
    all_day = df.loc[df["Period"] == "all_day"].reset_index(drop=True)

    # Manipulate the df for some of the metrics
    timeliness_df = timeliness_trips(df)
    sched_journey_vp = pct_vp_journey(
        all_day,
       "% Scheduled Trip w/ 1+ VP/Minute",
      "% Scheduled Trip w/ 2+ VP/Minute",
    )
    route_stats_df = route_stats(df)
    
    # Create the charts
    avg_scheduled_min_graph = (
        grouped_bar_chart(
            df=all_day,
            color_col="Direction",
            y_col="Average Scheduled Service (trip minutes)",
            offset_col="Direction",
            title=readable_dict["avg_scheduled_min_graph"]["title"],
            subtitle=readable_dict["avg_scheduled_min_graph"]["subtitle"],
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    timeliness_trips_dir_0 = (
            (
                base_facet_chart(
                    timeliness_df.loc[timeliness_df["dir_0_1"] == 0],
                    0,
                    "value",
                    "variable",
                    "Period",
                    readable_dict["timeliness_trips_graph"]["title"],
                    readable_dict["timeliness_trips_graph"]["subtitle"],
                )
            )
            .add_params(xcol_param)
            .transform_filter(xcol_param)
        )
    timeliness_trips_dir_1 = (
            (
                base_facet_chart(
                    timeliness_df.loc[timeliness_df["dir_0_1"] == 1],
                    1,
                    "value",
                    "variable",
                    "Period",
                    readable_dict["timeliness_trips_graph"]["title"],
                    "",
                )
            )
            .add_params(xcol_param)
            .transform_filter(xcol_param)
        )

    frequency_graph_dir_0 = (
    frequency_chart(df, 
                     0,
                     readable_dict["frequency_graph"]["title"],
                     readable_dict["frequency_graph"]["subtitle"],)
    .add_params(xcol_param)
    .transform_filter(xcol_param)
    )
    
    frequency_graph_dir_1 = (
    frequency_chart(df, 
                     1,
                     readable_dict["frequency_graph"]["title"],
                     "",)
    .add_params(xcol_param)
    .transform_filter(xcol_param)
    )
    
    speed_graph = (
        base_facet_line(
            df,
            "Speed (MPH)",
            readable_dict["speed_graph"]["title"],
            readable_dict["speed_graph"]["subtitle"],
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    vp_per_min_graph = (
        (
            base_facet_with_ruler_chart(
                all_day,
                "Average VP per Minute",
                "ruler_for_vp_per_min",
                readable_dict["vp_per_min_graph"]["title"],
                readable_dict["vp_per_min_graph"]["subtitle"],
                color_dict["vp_domain"],
                color_dict["vp_range"]
            )
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    sched_vp_per_min = (
        base_facet_circle(
            sched_journey_vp,
            "% of Actual Trip Minutes",
            "Category",
            "ruler_100_pct",
            readable_dict["sched_vp_per_min_graph"]["title"],
            readable_dict["sched_vp_per_min_graph"]["subtitle"],
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    spatial_accuracy = (
        base_facet_with_ruler_chart(
            all_day,
            "% VP within Scheduled Shape",
            "ruler_100_pct",
            readable_dict["spatial_accuracy_graph"]["title"],
            readable_dict["spatial_accuracy_graph"]["subtitle"],
            color_dict["spatial_accuracy_domain"],
            color_dict["spatial_accuracy_range"]
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    
    text_dir0 = (
            (create_text_table(route_stats_df, 
                               0,  
                               readable_dict["text_graph"]["title"],
                               readable_dict["text_graph"]["subtitle"]))
            .add_params(xcol_param)
            .transform_filter(xcol_param)
        )
    text_dir1 = (
            create_text_table(route_stats_df,
                              1, 
                             readable_dict["text_graph"]["title"],
                             "")
            .add_params(xcol_param)
            .transform_filter(xcol_param)
        )
    
    # Separate out the charts themetically.
    ride_quality = divider_chart(df, readable_dict["ride_quality_graph"]["title"])
    data_quality = divider_chart(df, readable_dict["data_quality_graph"]["title"])
    
    # Combine all the charts
    chart_list = [
    ride_quality,
    avg_scheduled_min_graph,
    timeliness_trips_dir_0,
    timeliness_trips_dir_1,
    frequency_graph_dir_0,
    frequency_graph_dir_1,
    speed_graph,
    data_quality,
    vp_per_min_graph,
    sched_vp_per_min,
    spatial_accuracy,
    text_dir0,
    text_dir1]

    chart = alt.vconcat(*chart_list)

    return chart
