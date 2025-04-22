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