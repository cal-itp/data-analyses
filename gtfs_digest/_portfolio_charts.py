import altair as alt
import pandas as pd
from omegaconf import OmegaConf
readable_dict = OmegaConf.load("new_readable.yml")

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


def circle_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str,
    color_scheme: list,
    tooltip_cols: list,
    date_format: str = "%b %Y",
    y_ticks: list = [0,1,2,3]
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
                scale=alt.Scale(domain=[0, y_ticks[-1]], nice=False),
                axis=alt.Axis(values=y_ticks)
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


def line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
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
            tooltip=tooltip_cols,
        )
    )
    return chart


def bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str,
    color_scheme: list,
    tooltip_cols: list,
    date_format: str = "%b %Y",
    y_ticks: list = [0, 30, 60, 90, 120, 150, 180],
) -> alt.Chart:

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                x_col,
                title=x_col,
                axis=alt.Axis(labelAngle=-45, format=date_format),
            ),
            y=alt.Y(
                y_col,
                title=y_col,
                scale=alt.Scale(domain=[0, y_ticks[-1]], nice=False),
                axis=alt.Axis(values=y_ticks),
            ),
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


def text_table(df: pd.DataFrame) -> alt.Chart:

    # Create the chart
    text_chart = (
        alt.Chart(df)
        .mark_text()
        .encode(x=alt.X("Zero:Q", axis=None), y=alt.Y("combo_col", axis=None))
    )

    text_chart = text_chart.encode(text="combo_col:N")

    return text_chart


def create_bg_service_chart() -> alt.Chart:
    """
    Create a shaded background for the Service Hour Chart
    to differentiate between time periods.
    """
    specific_chart_dict = readable_dict.background_graph
    cutoff = pd.DataFrame(
        {
            "start": [0, 4, 7, 10, 15, 19],
            "stop": [3.99, 6.99, 9.99, 14.99, 18.99, 24],
            "Time Period": [
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
    chart = (
        alt.Chart(cutoff.reset_index())
        .mark_rect(opacity=0.15)
        .encode(
            x="start",
            x2="stop",
            y=alt.value(0),
            y2=alt.value(250),
            color=alt.Color(
                "Time Period:N",
                sort=(
                    [
                        "Owl:12-3:59AM",
                        "Early AM:4-6:59AM",
                        "AM Peak:7-9:59AM",
                        "Midday:10AM-2:59PM",
                        "PM Peak:3-7:59PM",
                        "Evening:8-11:59PM",
                    ]
                ),
                scale=alt.Scale(
                    range=[*specific_chart_dict.colors]
                ),
            ),
        )
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