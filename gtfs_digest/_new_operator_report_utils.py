import _portfolio_charts
import altair as alt
import pandas as pd

from omegaconf import OmegaConf
readable_dict = OmegaConf.load("new_readable.yml")
"""
Prep Data
"""
def create_text_table(df: pd.DataFrame) -> pd.DataFrame:
    most_recent_date = df["Date"].max()
    most_recent_df = df.loc[
        (df.Date == most_recent_date) & (df["Day Type"] == "Weekday")
    ]
    text_table_df = most_recent_df.melt(
        id_vars=[
            "Date",
            "Route",
            "Direction",
        ],
        value_vars=[
            "Average Scheduled Minutes",
            "Average Scheduled Minutes",
            "Daily Trips All Day",
            "Headway All Day",
            "Headway Peak",
            "Headway Offpeak",
        ],
    ).sort_values(by=["Route", "Direction"])

    text_table_df.value = text_table_df.value.fillna(0)
    text_table_df.value = text_table_df.value.round(2)
    # text_table_df.value = text_table_df.value.astype(int)
    text_table_df.value = text_table_df.value.astype(str)
    text_table_df.value = text_table_df.value.replace("0", "N/A")

    text_table_df["Zero"] = 0

    text_table_df["combo_col"] = (
        text_table_df.variable.astype(str) + ": " + text_table_df.value.astype(str)
    )
    text_table_df["Direction"] = text_table_df.Direction.astype(str)

    return text_table_df
    
"""
Charts
"""
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

    
def create_hourly_summary(df: pd.DataFrame, day_type: str):
    
    chart_dict = readable_dict.hourly_summary
    df2 = df.loc[df["Day Type"] == "Saturday"]
    df2["Date"] = df["Date"].astype(str)
    
    date_list = list(df2["Date"].unique())
    
    date_dropdown = alt.binding_select(
        options=date_list,
        name="Dates: ",
    )
    xcol_param = alt.selection_point(
        fields=["Date"], value=date_list[0], bind=date_dropdown
    )

    chart = (
        (
            alt.Chart(df2)
            .mark_line(size=3)
            .encode(
                x=alt.X(
                    "Departure Hour",
                    title="Departure Hour",
                    axis=alt.Axis(
                        labelAngle=-45,
                    ),
                ),
                y=alt.Y(
                    "N Trips",
                    title="N Trips",
                ),
            )
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    
    bg = create_bg_service_chart()
    
    chart = (chart + bg).properties(
    resolve=alt.Resolve(
        scale=alt.LegendResolveMap(color=alt.ResolveMode("independent"))
    )
)
    chart = _report_operator_visuals.configure_chart(
    chart,
    width=400,
    height=250,
    title=f"{chart_dict.title} {day_type}",
    subtitle=chart_dict.subtitle)
    
    return chart

    
def create_route_dropdown(df: pd.DataFrame):
    routes_list = df["Route"].unique().tolist()
    route_dropdown = alt.binding_select(
        options=routes_list,
        name="Routes: ",
    )

    # Column that controls the bar charts
    xcol_param = alt.selection_point(
        fields=["Route"], value=routes_list[0], bind=route_dropdown
    )
    return xcol_param

def create_scheduled_minutes(df: pd.DataFrame):
    df2 = df.loc[df["Day Type"] == "Weekday"]
    chart_dict = readable_dict.avg_scheduled_minutes

    xcol_param = create_route_dropdown(df)

    dir_0_chart = _portfolio_charts.bar_chart(
        df=df2.loc[df2.Direction == 0],
        x_col="Date",
        y_col="Average Scheduled Minutes",
        color_col="Direction",
        color_scheme=[*chart_dict.colors],
        tooltip_cols=list(chart_dict.tooltip),
        date_format="",
        y_ticks=chart_dict.ticks,
    )

    dir_0_chart = (
        _portfolio_charts.configure_chart(
            dir_0_chart,
            width=200,
            height=250,
            title=f"{chart_dict.title} for Direction 0",
            subtitle=chart_dict.subtitle,
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    dir_1_chart = _portfolio_charts.bar_chart(
        df=df2.loc[df2.Direction == 1],
        x_col="Date",
        y_col="Average Scheduled Minutes",
        color_col="Direction",
        color_scheme=[*chart_dict.colors],
        tooltip_cols=list(chart_dict.tooltip),
        date_format="",
        y_ticks=chart_dict.ticks,
    )
    dir_1_chart = (
        _portfolio_charts.configure_chart(
            dir_1_chart,
            width=200,
            height=250,
            title="Direction 1",
            subtitle="",
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    chart = alt.hconcat(dir_0_chart, dir_1_chart)
    return chart

def create_scheduled_trips(df: pd.DataFrame):
    df2 = df.loc[df["Day Type"] == "Weekday"]
    chart_dict = readable_dict.scheduled

    xcol_param = create_route_dropdown(df)

    dir_0_chart = _portfolio_charts.bar_chart(
        df=df2.loc[df2.Direction == 0],
        x_col="Date",
        y_col="Daily Trips All Day",
        color_col="Direction",
        color_scheme=[*chart_dict.colors],
        tooltip_cols=list(chart_dict.tooltip),
        date_format="",
        y_ticks=chart_dict.ticks,
    )

    dir_0_chart = (
        _portfolio_charts.configure_chart(
            dir_0_chart,
            width=200,
            height=250,
            title=f"{chart_dict.title} for Direction 0",
            subtitle=chart_dict.subtitle,
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    dir_1_chart = _portfolio_charts.bar_chart(
        df=df2.loc[df2.Direction == 1],
        x_col="Date",
        y_col="Daily Trips All Day",
        color_col="Direction",
        color_scheme=[*chart_dict.colors],
        tooltip_cols=list(chart_dict.tooltip),
        date_format="",
        y_ticks=chart_dict.ticks,
    )
    dir_1_chart = (
        _portfolio_charts.configure_chart(
            dir_1_chart,
            width=200,
            height=250,
            title="Direction 1",
            subtitle="",
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    chart = alt.hconcat(dir_0_chart, dir_1_chart)
    return chart

def create_frequency(df: pd.DataFrame):
    df2 = df.loc[df["Day Type"] == "Weekday"]
    chart_dict = readable_dict.frequency

    xcol_param = create_route_dropdown(df)

    dir_0_chart = _portfolio_charts.bar_chart(
        df=df2.loc[df2.Direction == 0],
        x_col="Date",
        y_col="Headway All Day",
        color_col="Direction",
        color_scheme=[*chart_dict.colors],
        tooltip_cols=list(chart_dict.tooltip),
        date_format="",
        y_ticks=chart_dict.ticks,
    )

    dir_0_chart = (
        _portfolio_charts.configure_chart(
            dir_0_chart,
            width=200,
            height=250,
            title=f"{chart_dict.title} for Direction 0",
            subtitle=chart_dict.subtitle,
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )

    dir_1_chart = _portfolio_charts.bar_chart(
        df=df2.loc[df2.Direction == 1],
        x_col="Date",
        y_col="Headway All Day",
        color_col="Direction",
        color_scheme=[*chart_dict.colors],
        tooltip_cols=list(chart_dict.tooltip),
        date_format="",
        y_ticks=chart_dict.ticks,
    )
    dir_1_chart = (
        _portfolio_charts.configure_chart(
            dir_1_chart,
            width=200,
            height=250,
            title="Direction 1",
            subtitle="",
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
    )
    chart = alt.hconcat(dir_0_chart, dir_1_chart)
    return chart

def create_text_graph(df: pd.DataFrame):
    chart_dict = readable_dict.avg_scheduled_minutes
    
    df2 = create_text_table(df)

    # Create dropdown menus
    options = ["0", "1"]
    input_dropdown = alt.binding_radio(
        # Add the empty selection which shows all when clicked
        options=options,
        labels=options,
        name="Direction: ",
    )
    selection = alt.selection_point(
        fields=["Direction"],
        value=options[0],
        bind=input_dropdown,
    )

    xcol_param = create_route_dropdown(df2)

    chart = (
        (
            _portfolio_charts.configure_chart(
                text_table(df2),
                width=400,
                height=250,
                title=chart_dict.title,
                subtitle=chart_dict.subtitle,
            )
        )
        .add_params(xcol_param)
        .transform_filter(xcol_param)
        .add_params(selection)
        .transform_filter(selection)
    )

    return chart