import pandas as pd

# Charts
import altair as alt
alt.data_transformers.enable('default', max_rows=None)

# Other
from segment_speed_utils.project_vars import RT_SCHED_GCS, SCHED_GCS
from shared_utils import catalog_utils, rt_dates, rt_utils

# Data Dictionary
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

# Yaml
from omegaconf import OmegaConf
readable_dict = OmegaConf.load("readable2.yml")

"""
Configure
"""
def configure_chart(
    chart: alt.Chart, width: int, height: int, title: str, subtitle: str
) -> alt.Chart:
    """
    Adjust width, height, title, and subtitle
    """
    chart2 = chart.properties(
        width=width,
        height=height,
        title={
            "text": [title],
            "subtitle": [subtitle],
        },
    )
    return chart2

"""
Functions to Reshape DF
"""
def reshape_timeliness_trips(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape dataframe for the charts that illustrate
    how timely a route's trips are.
    """
    melted_df = df.melt(
        id_vars=[
            "Date",
            "Portfolio Organization Name",
            "Route",
            "Period",
            "Direction",
            "Direction (0/1)",
            "# Realtime Trips",
        ],
        value_vars=[
            "# Early Arrival Trips",
            "# On-Time Trips",
            "# Late Trips",
        ],
    )

    melted_df["Percentage"] = (melted_df.value / melted_df["# Realtime Trips"]) * 100

    return melted_df

def reshape_pct_journey_with_vp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape the data for the charts that display the % of
    a journey that recorded 2+ vehicle positions/minute.
    """
    to_keep = [
        "Date",
        "Portfolio Organization Name",
        "Direction",
        "% Scheduled Trip w/ 1+ VP/Minute",
        "% Scheduled Trip w/ 2+ VP/Minute",
        "Route",
        "Period",
    ]
    df2 = df[to_keep]

    df3 = df2.melt(
        id_vars=[
            "Date",
            "Portfolio Organization Name",
            "Route",
            "Direction",
            "Period",
        ],
        value_vars=[
            "% Scheduled Trip w/ 1+ VP/Minute",
            "% Scheduled Trip w/ 2+ VP/Minute",
        ],
    )

    df3 = df3.rename(columns={"variable": "Category", "value": "% of Trip Duration"})

    return df3

def reshape_route_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find overall statistics for a route.
    This dataframe backs the last two text table charts.
    """
    most_recent_date = df["Date"].max()
    route_merge_cols = ["Route", "Direction", "Direction (0/1)"]

    # Filter out for the most recent date.
    # Create 3 separate dataframes for all day, peak, and offpeak.
    all_day_stats = df[(df["Date"] == most_recent_date) & (df["Period"] == "All Day")][
        route_merge_cols
        + [
            "Average Scheduled Service (trip minutes)",
            "Average Stop Distance (Miles)",
            "# Scheduled Trips",
            "GTFS Availability",
        ]
    ]

    peak_stats = df[(df["Date"] == most_recent_date) & (df["Period"] == "Peak")][
        route_merge_cols + ["Speed (MPH)", "# Scheduled Trips", "Headway (Minutes)"]
    ].rename(
        columns={
            "Speed (MPH)": "Peak Avg Speed (MPH)",
            "# Scheduled Trips": "peak_scheduled_trips",
            "Headway (Minutes)": "Peak Headway (Minutes)",
        }
    )

    offpeak_stats = df[(df["Date"] == most_recent_date) & (df["Period"] == "Offpeak")][
        route_merge_cols + ["Speed (MPH)", "# Scheduled Trips", "Headway (Minutes)"]
    ].rename(
        columns={
            "Speed (MPH)": "Offpeak Avg Speed (MPH)",
            "# Scheduled Trips": "offpeak_scheduled_trips",
            "Headway (Minutes)": "Offpeak Headway (Minutes)",
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

    # Add back date
    table_df["Date"] = most_recent_date
    return table_df

def reshape_df_text_table(df: pd.DataFrame) -> pd.DataFrame:

    # Create the dataframe first
    route_stats_df = reshape_route_stats(df)

    # Reshape dataframe before plotting
    melt1 = route_stats_df.melt(
        id_vars=[
            "Date",
            "Route",
            "Direction",
            "Direction (0/1)",
        ],
        value_vars=[
            "Average Scheduled Service (Trip Minutes)",
            "Average Stop Distance (Miles)",
            "# Scheduled Trips",
            "Gtfs Availability",
            "Peak Avg Speed (Mph)",
            "Peak Scheduled Trips",
            "Peak Headway (Minutes)",
            "Offpeak Avg Speed (Mph)",
            "Offpeak Scheduled Trips",
            "Offpeak Headway (Minutes)",
        ],
    )

    # Create a decoy column so all the text will be centered.
    melt1["Zero"] = 0

    # Combine columns so the column title and variable will be aligned.
    # Ex: "Trips Per Hour: 0.56". This column is what will show up on the
    # graphs.
    melt1["combo_col"] = melt1.variable.astype(str) + ": " + melt1.value.astype(str)

    # Clean up
    melt1.combo_col = melt1.combo_col.str.replace(
        "schedule_and_vp",
        "Schedule and Realtime Data",
    ).str.replace("Gtfs", "GTFS")

    return melt1

def reshape_timeliness_trips(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape dataframe for the charts that illustrate
    how timely a route's trips are.
    """

    melted_df = df.melt(
        id_vars=[
            "Date",
            "Portfolio Organization Name",
            "Route",
            "Period",
            "Direction",
            "Direction (0/1)",
            "# Realtime Trips",
        ],
        value_vars=[
            "# Early Arrival Trips",
            "# On-Time Trips",
            "# Late Trips",
        ],
    )

    melted_df["Percentage"] = (melted_df.value / melted_df["# Realtime Trips"]) * 100

    return melted_df
"""
Charts
"""
def ruler_chart(
    df: pd.DataFrame, 
    ruler_value: int
) -> pd.DataFrame:
    # Add column with ruler value, use .assign to avoid warning
    df = df.assign(
        ruler = ruler_value
    )
    
    chart = (
        alt.Chart(df)
        .mark_rule(color="red", strokeDash=[10, 7])
        .encode(y="mean(ruler):Q")
    )
    
    return chart

def pie_chart(
    df: pd.DataFrame, color_col: str, theta_col: str, color_scheme: list, tooltip_cols:list
) -> alt.Chart:
    chart = (
        alt.Chart(df)
        .mark_arc()
        .encode(
            theta=theta_col,
            color=alt.Color(
                color_col,
                title=(color_col),
                scale=alt.Scale(range=color_scheme),
            ),
            tooltip=tooltip_cols,
        )
    )

    return chart

def bar_chart(
    x_col: str,
    y_col: str,
    color_col: str,
    color_scheme: list,
    tooltip_cols: list,
    date_format: str = "%b %Y",
) -> alt.Chart:

    chart = (
        alt.Chart()
        .mark_bar()
        .encode(
            x=alt.X(
                x_col,
                title=x_col,
                axis=alt.Axis(labelAngle=-45, format=date_format),
            ),
            y=alt.Y(y_col, title=y_col),
            color=alt.Color(
                color_col,
                legend=None,
                title=color_col,
                scale=alt.Scale(range=color_scheme),
            ),
            tooltip=tooltip_cols,
        )
    )

    return chart

def line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str,
    color_scheme: list,
    tooltip_cols: list,
    date_format: str = "%b %Y",
):
    # Set chart
    chart = (
        alt.Chart(df)
        .mark_line(size=3)
        .encode(
            x=alt.X(
                x_col,
                title=x_col,
                axis=alt.Axis(labelAngle=-45, format=date_format),
            ),
            y=alt.Y(
                f"{y_col}:Q",
                title=y_col,
            ),
            color=alt.Color(
                f"{color_col}:N",
                title=color_col,
                scale=alt.Scale(range=color_scheme),
            ),
            tooltip=tooltip_cols,
        )
    )
    return chart

def divider_chart(df: pd.DataFrame, title: str):
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
            text=title,
        )
        .properties(width=400, height=100)
    )

    return chart

def text_table(df: pd.DataFrame) -> alt.Chart:

    # Create the chart
    text_chart = (
        alt.Chart(df)
        .mark_text()
        .encode(x=alt.X("Zero:Q", axis=None), y=alt.Y("combo_col", axis=None))
    )

    text_chart = text_chart.encode(text="combo_col:N")

    return text_chart

def grouped_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str,
    color_scheme: list,
    tooltip_cols: list,
    date_format: str,
    offset_col: str,
) -> alt.Chart:
    chart = bar_chart(
        x_col, y_col, color_col, color_scheme, tooltip_cols, date_format
    )
    # Add Offset
    chart2 = chart.mark_bar(size=5).encode(
        xOffset=alt.X(offset_col, title=offset_col),
    ).properties(data=df)
    
    return chart2

def circle_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str,
    color_scheme: list,
    tooltip_cols: list,
    date_format: str = "%b %Y",
) -> alt.Chart:

    chart = (
        alt.Chart(df)
        .mark_circle(size=150)
        .encode(
            x=alt.X(
                x_col,
                title=(x_col),
                axis=alt.Axis(labelAngle=-45, format=date_format),
            ),
            y=alt.Y(
                y_col,
                title=(y_col),
            ),
            color=alt.Color(
                color_col,
                title=(color_col),
                scale=alt.Scale(range=color_scheme),
            ),
            tooltip=tooltip_cols
        )
    )
    
    return chart 

"""
Actual Charts
"""
def sample_spatial_accuracy_chart(df):
    specific_chart_dict = readable_dict.spatial_accuracy_graph

    ruler = ruler_chart(df, 100)

    bar = bar_chart(
        x_col = "Date", 
        y_col = "% VP within Scheduled Shape", 
        color_col = "% VP within Scheduled Shape", 
        color_scheme = [*specific_chart_dict.colors], 
        tooltip_cols = [*specific_chart_dict.tooltip], 
        date_format="%b %Y"
    )
   
    # write this way so that the df is inherited by .facet
    chart = alt.layer(bar, ruler, data = df).properties(width=200, height=250)
    chart = chart.facet(
        column=alt.Column(
            "Direction:N",
        )
    ).properties(
        title={
            "text": specific_chart_dict.title,
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    
    return chart


def sample_avg_scheduled_min_chart(df):
    specific_chart_dict = readable_dict.avg_scheduled_min_graph
    
    chart = grouped_bar_chart(
        df,
        x_col = "Date:T",
        y_col="Average Scheduled Service (trip minutes)",
        color_col="Direction:N",
        color_scheme = [*specific_chart_dict.colors],
        tooltip_cols = [*specific_chart_dict.tooltip],
        date_format = "%b %Y",
        offset_col="Direction:N",
    )
        
    chart = configure_chart(
        chart,
        width = 400, height = 250, 
        title = specific_chart_dict.title, 
        subtitle = specific_chart_dict.subtitle
    )    
    
    return chart

def vp_per_minute_chart(df: pd.DataFrame) -> alt.Chart:
    specific_chart_dict = readable_dict.vp_per_min_graph
    ruler = ruler_chart(df, 3)

    bar = bar_chart(
        x_col="Date",
        y_col="Average VP per Minute",
        color_col="Average VP per Minute",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
        date_format="%b %Y",
    )

    # write this way so that the df is inherited by .facet
    chart = alt.layer(bar, ruler, data=df).properties(width=200, height=250)
    chart = chart.facet(
        column=alt.Column(
            "Direction:N",
        )
    ).properties(
        title={
            "text": specific_chart_dict.title,
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    return chart

def timeliness_chart(df) -> alt.Chart:

    # Reshape dataframe from wide to long
    df2 = reshape_timeliness_trips(df)

    specific_chart_dict = readable_dict.timeliness_trips_graph

    chart = line_chart(
        df=df2,
        x_col="Date",
        y_col="Percentage",
        color_col="variable",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
    ).properties(width=200, height=250)

    chart = chart.facet(
        column=alt.Column(
            "Direction:N",
        )
    ).properties(
        title={
            "text": specific_chart_dict.title,
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    return chart

def speed_chart(df) -> alt.Chart:
    specific_chart_dict = readable_dict.speed_graph

    chart = line_chart(
        df=df,
        x_col="Date",
        y_col="Speed (MPH)",
        color_col="Period",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
    ).properties(width=200, height=250)

    chart = chart.facet(
        column=alt.Column(
            "Direction:N",
        )
    ).properties(
        title={
            "text": specific_chart_dict.title,
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    return chart

def sched_vp_per_min_chart(df) -> alt.Chart:

    # Change df from wide to long
    pct_journey_with_vp_df = reshape_pct_journey_with_vp(df)
    specific_chart_dict = readable_dict.sched_vp_per_min_graph

    ruler = ruler_chart(pct_journey_with_vp_df,100)
    
    circle = circle_chart(
        df=pct_journey_with_vp_df,
        x_col="Date",
        y_col="% of Trip Duration",
        color_col="Category",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
    )

    chart = alt.layer(circle, ruler, data= pct_journey_with_vp_df).properties(width=200, height=250)
    
    chart = chart.facet(
        column=alt.Column(
            "Direction:N",
        )
    ).properties(
        title={
            "text": specific_chart_dict.title,
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    return chart

def text_chart(df: pd.DataFrame, direction: int) -> alt.Chart:

    specific_chart_dict = readable_dict.text_graph

    # Filter to one direction only
    df2 = df.loc[df["Direction (0/1)"] == direction]

    # Reshape df for text table and filter for only one direction
    text_table_df = reshape_df_text_table(df2)

    chart = text_table(text_table_df)

    # Grab cardinal direction value to use for the title
    direction_str = text_table_df["Direction"].iloc[0]

    # Grab most recent date
    date_str = text_table_df["Date"].iloc[0].strftime("%B %Y")

    # write this way so that the df is inherited by .facet
    chart = configure_chart(
        chart,
        width=400,
        height=250,
        title=f"{specific_chart_dict.title}{direction_str} Vehicles",
        subtitle=f"{specific_chart_dict.subtitle} {date_str}",
    )
    return chart

def timeliness_chart(df: pd.DataFrame, direction: int) -> alt.Chart:

    # Filter to one direction only
    df = df.loc[df["Direction (0/1)"] == direction]

    # Reshape dataframe from wide to long
    df2 = reshape_timeliness_trips(df)

    # Grab cardinal direction value to use for the title
    direction_str = df2["Direction"].iloc[0]

    specific_chart_dict = readable_dict.timeliness_trips_graph

    chart = line_chart(
        df=df2,
        x_col="Date",
        y_col="Percentage",
        color_col="variable",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
    ).properties(width=200, height=250)

    chart = chart.facet(
        column=alt.Column(
            "Direction:N",
        )
    ).properties(
        title={
            "text": f"{specific_chart_dict.title}{direction_str} Vehicles",
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    return chart

def total_scheduled_trips_chart(df: pd.DataFrame, direction: int) -> alt.Chart:
    # Filter to one direction only
    df = df.loc[df["Direction (0/1)"] == direction]

    # Grab cardinal direction value to use for the title
    direction_str = df["Direction"].iloc[0]

    specific_chart_dict = readable_dict.n_scheduled_graph

    chart = bar_chart(
        x_col="Date:T",
        y_col="# Scheduled Trips",
        color_col="Period:N",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
        date_format="%b %Y",
    )

    chart = alt.layer(chart, data=df)

    # write this way so that the df is inherited by .facet
    chart = configure_chart(
        chart,
        width=400,
        height=250,
        title=f"{specific_chart_dict.title}{direction_str} Vehicles",
        subtitle=specific_chart_dict.subtitle,
    )
    return chart

def headway_chart(df: pd.DataFrame, direction: int) -> alt.Chart:

    # Filter to one direction only
    df = df.loc[df["Direction (0/1)"] == direction]

    # Grab cardinal direction value to use for the title
    direction_str = df["Direction"].iloc[0]

    specific_chart_dict = readable_dict.frequency_graph

    chart = bar_chart(
        x_col="Date:T",
        y_col="Headway (Minutes)",
        color_col="Headway (Minutes):N",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
        date_format="%b %Y",
    )

    chart = (
        alt.layer(chart, data=df)
        .encode(y=alt.Y("Headway (Minutes)", scale=alt.Scale(domain=[0, 250])))
        .properties(width=200, height=250)
    )

    chart = chart.facet(
        column=alt.Column(
            "Period:N",
        )
    ).properties(
        title={
            "text": f"{specific_chart_dict.title} {direction_str} Vehicles",
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    return chart

def route_filter(df):
    routes_list = df["Route"].unique().tolist()

    route_dropdown = alt.binding_select(
        options=routes_list,
        name="Routes: ",
    )
    # Column that controls the bar charts
    xcol_param = alt.selection_point(
        fields=["Route"], value=routes_list[0], bind=route_dropdown
    )

    # Charts
    spatial_accuracy = (
        sample_spatial_accuracy_chart(df[df.Period == "All Day"])
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    avg_scheduled_min = (
       sample_avg_scheduled_min_chart(df[df.Period == "All Day"])
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    vp_per_minute = (
        vp_per_minute_chart(df[df.Period == "All Day"])
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    speed = (
        speed_chart(df)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    sched_vp_per_min = (
        sched_vp_per_min_chart(df[df.Period == "All Day"])
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    text_dir0 = (
        text_chart(df, 0)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    text_dir1 = (
        text_chart(df, 1)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    
    timeliness_dir0 =(
        timeliness_chart(df[df.Period == "All Day"], 0)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    
    timeliness_dir1 =(
        timeliness_chart(df[df.Period == "All Day"], 1)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    
    n_scheduled_dir0 = (
        total_scheduled_trips_chart(df[df.Period == "All Day"], 0)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    
    n_scheduled_dir1 = (
        total_scheduled_trips_chart(df[df.Period == "All Day"], 1)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    
    n_freq_dir0 = (
        headway_chart(df[df.Period != "All Day"], 0)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    
    n_freq_dir1 = (
         headway_chart(df[df.Period != "All Day"], 1)
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    # Divider Charts
    data_quality = divider_chart(
        df, readable_dict.data_quality_graph.title
    )
    rider_quality = divider_chart(
        df, readable_dict.rider_quality_graph.title
    )
    summary = divider_chart(df, readable_dict.summary_graph.title)

    chart_list = [
        summary,
        text_dir0,
        text_dir1,
        rider_quality,
        avg_scheduled_min,
        timeliness_dir0,
        timeliness_dir1,
        n_freq_dir0,
        n_scheduled_dir0,
        n_freq_dir1,
        n_scheduled_dir1,
        speed,
        data_quality,
        spatial_accuracy,
        vp_per_minute,
        
        sched_vp_per_min,
    ]
    chart = alt.vconcat(*chart_list)

    return chart