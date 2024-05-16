import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd
import numpy as np

# Charts
import altair as alt
alt.data_transformers.enable('default', max_rows=None)
import _report_utils

# Great Tables
import great_tables as gt
from great_tables import md

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
Schedule_vp_metrics
Functions
"""
def load_most_current_date() -> str:
    # from shared_utils import rt_utils
    dates_dictionary = rt_dates.DATES
    date_list = list(dates_dictionary.items())
    # Grab the last key-value pair
    last_key, last_value = date_list[-1]
    return last_value

def load_scheduled_stop_times(date: str, gtfs_schedule_key: list) -> pd.DataFrame:
    stop_times_col = [
        "feed_key",
        "stop_id",
        "stop_sequence",
        "schedule_gtfs_dataset_key",
        "trip_instance_key",
        "shape_array_key",
        "stop_name",
        "prior_stop_sequence",
        "subseq_stop_sequence",
        "stop_pair",
        "stop_pair_name",
        "stop_primary_direction",
        "stop_meters",
    ]
    stop_times_df = helpers.import_scheduled_stop_times(
        date,
        filters=[[("schedule_gtfs_dataset_key", "in", gtfs_schedule_key)]],
        columns=stop_times_col,
        get_pandas=True,
        with_direction=True,
    ).assign(service_date=pd.to_datetime(date))

    return stop_times_df

def load_scheduled_trips(date: str, gtfs_schedule_key: list) -> pd.DataFrame:
    scheduled_col = [
        "route_id",
        "trip_instance_key",
        "gtfs_dataset_key",
        "shape_array_key",
        "direction_id",
        "route_long_name",
        "route_short_name",
        "route_desc",
        "name",
    ]

    scheduled_trips_df = helpers.import_scheduled_trips(
        date,
        filters=[[("gtfs_dataset_key", "in", gtfs_schedule_key)]],
        columns=scheduled_col,
    ).assign(service_date=pd.to_datetime(date))

    return scheduled_trips_df

def load_stack_all_dates(date_list: list, gtfs_schedule_keys: list) -> pd.DataFrame:
    scheduled_stop_times_df = pd.DataFrame()
    # list comprehension
    # df = delayed(pd.read_parquet(one_date).assing(service_date))
    # aggregated = common_route_dir(df) group_cols includes date
    # pd.concat() -- can you concat delayed objects though?
    
    
    for i in date_list:
        df = load_scheduled_stop_times(i, gtfs_schedule_keys)
        scheduled_stop_times_df = pd.concat([scheduled_stop_times_df, df], axis=0)

    scheduled_trips_df = pd.DataFrame()
    for i in date_list:
        df = load_scheduled_trips(i, gtfs_schedule_keys)
        scheduled_trips_df = pd.concat([scheduled_trips_df, df], axis=0)
    # get stop_times and trips (keep date)
    # concatenate
    # aggregate

    return scheduled_stop_times_df, scheduled_trips_df

def find_most_common_dir(df: pd.DataFrame) -> pd.DataFrame:
    # Count total stops
    agg1 = (
        df.groupby(
            [
                "route_id",
                "schedule_gtfs_dataset_key",
                "direction_id",
                "stop_primary_direction",
            ]
        )
        .agg({"stop_sequence": "count"})
        .reset_index()
        .rename(columns={"stop_sequence": "total_stops"})
    )

    # Sort and drop duplicates so that the
    # largest # of stops by stop_primary_direction is at the top
    agg2 = agg1.sort_values(
        by=["route_id", "schedule_gtfs_dataset_key", "direction_id", "total_stops"],
        ascending=[True, True, True, False],
    )

    # Drop duplicates so only the top stop_primary_direction is kept.
    agg3 = agg2.drop_duplicates(
        subset=[
            "route_id",
            "schedule_gtfs_dataset_key",
            "direction_id",
        ]
    ).reset_index(drop=True)

    agg3 = agg3.drop(columns=["total_stops"])
    return agg3

def find_most_recent_route_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        route_id=df.route_id.fillna(""),
        route_short_name=df.route_short_name.fillna(""),
        route_long_name=df.route_long_name.fillna(""),
    )

    df = df.assign(combined_name=df.route_short_name + "__" + df.route_long_name)

    df = df.assign(
        route_id2=df.apply(
            lambda x: gtfs_schedule_wrangling.standardize_route_id(
                x, "name", "route_id"
            ),
            axis=1,
        )
    )

    route_cols = ["schedule_gtfs_dataset_key", "name", "route_id2"]

    df2 = gtfs_schedule_wrangling.most_recent_route_info(
        df, group_cols=route_cols, route_col="combined_name"
    ).pipe(
        gtfs_schedule_wrangling.most_recent_route_info,
        group_cols=["schedule_gtfs_dataset_key", "name", "recent_combined_name"],
        route_col="route_id2",
    )
    
    sort_order = [True for c in route_cols]
    df3 = (
        df2.sort_values(route_cols + ["service_date"], ascending=sort_order + [False])
        .drop_duplicates(subset=route_cols)
        .rename(columns={"combined_name": "recent_combined_name"})
    )

    df3 = df3[["schedule_gtfs_dataset_key", "recent_route_id2", "route_id"]]
    return df3

def find_cardinal_direction(date_list: list, gtfs_schedule_keys: list) -> pd.DataFrame:
    # Grab all available dates for these dataframes
    scheduled_stop_times_df, scheduled_trips_df = load_stack_all_dates(
        date_list, gtfs_schedule_keys
    )

    # Merge them
    m1 = pd.merge(
        scheduled_trips_df,
        scheduled_stop_times_df,
        on=["trip_instance_key", "schedule_gtfs_dataset_key", "shape_array_key"],
        how="inner",
    )

    # Find the most common direction for this Route ID
    common_stops_df = find_most_common_dir(m1)

    # Find the most recent Route ID to connect back to sched_vp_df
    recent_ids_df = find_most_recent_route_id(scheduled_trips_df)

    # Merge this
    m2 = pd.merge(
        common_stops_df,
        recent_ids_df,
        on=["schedule_gtfs_dataset_key", "route_id"],
        how="inner",
    )
    
    m2 = m2.drop(columns = ["route_id"])
    return m2

def load_schedule_vp_metrics(organization:str)->pd.DataFrame:
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
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
    
    pct_cols = df.columns[df.columns.str.contains("pct")].tolist()
    for i in pct_cols:
        df[i] = df[i] * 100
        
    # Add rulers
    df["ruler_100_pct"] = 100
    df["ruler_for_vp_per_min"] = 2
    
    # Add a column that flips frequency to be every X minutes instead
    # of every hour.
    df["frequency_in_minutes"] = 60/df.frequency
    
    # Replace column names
    df.columns = df.columns.map(_report_utils.replace_column_names)
    
    # Replace 0/1 in Direction with cardinal direction
    # First only grab the most recent date's gtfs_dataset_key
    all_dates_list = list(df.Date.unique())
    gtfs_keys = list(df.schedule_gtfs_dataset_key.unique())
    all_dates_list = [np.datetime_as_string(date, unit="D") for date in all_dates_list]
    
    # Find cardinal direction
    cardinal_direction_df = find_cardinal_direction(all_dates_list, gtfs_keys)
    
    # Left merge to keep  
    m1 = pd.merge(
    df,
    cardinal_direction_df,
    left_on=["schedule_gtfs_dataset_key", "Direction", "Route ID"],
    right_on=[
        "schedule_gtfs_dataset_key",
        "direction_id",
        "recent_route_id2",
    ],
    how="left"
    )
    
    # Clean up
    m1 = m1.rename(
    columns={"Direction":"dir_0_1",
             "stop_primary_direction":"Direction"}
    )
    
    return m1


def route_stats(df: pd.DataFrame) -> pd.DataFrame:
    most_recent_date = df["Date"].max()
    route_merge_cols = ["Route", "Direction", "dir_0_1"]

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

    numeric_cols = table_df.select_dtypes(include="number").columns
    table_df[numeric_cols] = table_df[numeric_cols].fillna(0)
    table_df.columns = table_df.columns.str.title().str.replace("_", " ")
    return table_df

def timeliness_trips(df: pd.DataFrame):
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
    df = df.loc[df["Period"] != "All Day"]
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
Operator Level
"""
def load_operator_schedule_rt_category(schedule_gtfs_key: list) -> pd.DataFrame:
    df = pd.read_parquet(
        f"{RT_SCHED_GCS}digest/operator_schedule_rt_category.parquet",
        filters=[[("schedule_gtfs_dataset_key", "in", schedule_gtfs_key)]],
    )
    df.n_trips = df.n_trips.astype(int).fillna(0)
    return df


"""
Charts
"""
def divider_chart(df: pd.DataFrame, text):
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

def create_data_unavailable_chart():

    chart = alt.LayerChart()
    
    return chart

def clean_data_charts(df:pd.DataFrame, y_col:str)->pd.DataFrame:
    df["Period"] = df["Period"].str.replace("_", " ").str.title()
    df[y_col] = df[y_col].fillna(0).astype(int)
    df[f"{y_col}_str"] = df[y_col].astype(str)
    
    return df

def set_y_axis(df, y_col):
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
):
    tooltip_cols = [
        "Period",
        "Route",
        "Organization",
        "Date",
        "Direction",
        color_col,
        y_col,
    ]

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
                scale=alt.Scale(range=color_dict["tri_color"]),
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
    df: pd.DataFrame, y_col: str, title: str, subtitle: str
) -> alt.Chart:
    max_y = set_y_axis(df, y_col)

    df = clean_data_charts(df, y_col)
    tooltip_cols = [
            "Route",
            "Period",
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

    max_y = set_y_axis(df, y_col)
    df = clean_data_charts(df, y_col)
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
    y_col: str,
    color_col: str,
    facet_col: str,
    title: str,
    subtitle: str,
):
    tooltip_cols = [
        "Period",
        "Route",
        "Organization",
        "Date",
        "Direction",
        y_col,
        color_col,
    ]

    max_y =set_y_axis(df, y_col)
    df = clean_data_charts(df, y_col)
    
    direction = df["Direction"].values[0]
    title = title + direction
    
    chart = (
        (
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
                    scale=alt.Scale(range=color_dict["red_green_yellow"]),
                ),
                tooltip=tooltip_cols,
            )
        ))
    chart = chart.properties(width=200, height=250)
    chart = chart.facet(
            column=alt.Column(
                f"{facet_col}:N",
            )
        ).properties(
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
):
    tooltip_cols = [
        "Period",
        "Route",
        "Organization",
        "Date",
        "Direction",
        y_col,
    ]

    max_y = set_y_axis(df, y_col)
    df = clean_data_charts(df, y_col)
    
    color_scale = alt.Scale(
    domain= domain_color,
    range = range_color
    )
    
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

def create_text_table(df: pd.DataFrame, direction: int):

    df = df.loc[df["Dir 0 1"] == direction].drop_duplicates().reset_index(drop=True)
    
    direction_title = df["Direction"].values[0]
   
    
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
    # Create a decoy column to center all the text
    df2["Zero"] = 0

    df2["combo_col"] = df2.variable.astype(str) + ": " + df2.value.astype(str)
    df2.combo_col = df2.combo_col.str.replace(
            "schedule_and_vp", "Schedule and Realtime Data",
        ).str.replace("Gtfs", "GTFS")
    text_chart = (
            alt.Chart(df2)
            .mark_text()
            .encode(x=alt.X("Zero:Q", axis=None), y=alt.Y("combo_col", axis=None))
        )
    
    text_chart = text_chart.encode(text="combo_col:N").properties(
            title=f"Route Statistics for {direction_title}",
            width=400,
            height=250,
        )
    return text_chart
    
def frequency_chart(df: pd.DataFrame):
    df["Frequency in Minutes"] = (
        "A trip going this direction comes every "
        + df.frequency_in_minutes.astype(int).astype(str)
        + " minutes"
    )
    
    # Define the fixed x-axis values
    fixed_x_values = [0,30,60,90,120,150,180,210,240]
    
    color_scale = alt.Scale(
        domain= color_dict["freq_domain"],
        range = color_dict["freq_range"]
    )
    
    chart = (
        alt.Chart(df)
        .properties(width=180, height=alt.Step(10))
        .mark_bar()
        .encode(
            alt.Y(
                "yearmonthdate(Date):O",
                title="Date",
                axis=alt.Axis(format="%b %Y"),
            ),
            alt.X(
                "frequency_in_minutes:Q",
                title=_report_utils.labeling("frequency_in_minutes"),
                axis=alt.Axis(values=fixed_x_values, title="Frequency in Minutes")
            ),
            alt.Color(
                "frequency_in_minutes:Q",
                scale=color_scale,
            ).title(_report_utils.labeling("frequency_in_minutes")),
            alt.Row("Period:N")
            .title(_report_utils.labeling("Period"))
            .header(labelAngle=0),
            alt.Column("Direction:N").title(_report_utils.labeling("Direction")),
            tooltip=["Date", "Route", "Frequency in Minutes", "Period", "Direction",],
        )
    )
    chart = chart.properties(
        title={
            "text": readable_dict["frequency_graph"]["title"],
            "subtitle": readable_dict["frequency_graph"]["subtitle"],
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

    # Filter for only rows categorized as found in schedule and vp and all_day
    all_day = df.loc[df["Period"] == "all_day"].reset_index(drop=True)

    # Create route stats table for the text tables
    route_stats_df = route_stats(df)

    # Manipulate the df for some of the metrics
    timeliness_df = timeliness_trips(df)

    sched_journey_vp = pct_vp_journey(
        all_day,
       "% Scheduled Trip w/ 1+ VP/Minute",
      "% Scheduled Trip w/ 2+ VP/Minute",
    )

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
    # display(avg_scheduled_min_graph)
    timeliness_trips_dir_0 = (
        (
            base_facet_chart(
                timeliness_df.loc[timeliness_df["dir_0_1"] == 0],
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
    # display(timeliness_trips_dir_0)
    timeliness_trips_dir_1 = (
        (
            base_facet_chart(
                timeliness_df.loc[timeliness_df["dir_0_1"] == 1],
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
    # display(timeliness_trips_dir_1)
    frequency_graph = (
    frequency_chart(df)
    .add_params(xcol_param)
    .transform_filter(xcol_param)
    )
    
    # display(frequency_graph)
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
    # display(speed_graph)
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

    # display(rt_vp_per_min_graph)
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
    # display(sched_vp_per_min)
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
    
    # display(spatial_accuracy)
    text_dir0 = (
        (create_text_table(route_stats_df, 0))
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    # display(text_dir0)
    text_dir1 = (
        create_text_table(route_stats_df, 1)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    # display(text_dir1)
    
    ride_quality = divider_chart(df, "The charts below measure the quality of the rider experience for this route.")
    data_quality = divider_chart(df, "The charts below describe the quality of GTFS data collected for this route.")
    chart_list = [
        ride_quality,
        avg_scheduled_min_graph,
        timeliness_trips_dir_0,
        timeliness_trips_dir_1,
        frequency_graph,
        speed_graph,
        data_quality,
        vp_per_min_graph,
        sched_vp_per_min,
        spatial_accuracy,
        text_dir0,
        text_dir1
    ]

     
    """ chart = alt.vconcat(*chart_list).properties(
        resolve=alt.Resolve(
            scale=alt.LegendResolveMap(color=alt.ResolveMode("independent"))
        )
    )
    """
    chart = alt.vconcat(*chart_list)

    return chart
