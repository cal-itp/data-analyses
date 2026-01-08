import _portfolio_charts
import deploy_portfolio_yaml
import altair as alt
import pandas as pd
import pandas_gbq
import numpy as np

from omegaconf import OmegaConf
readable_dict = OmegaConf.load("new_readable.yml")

import google.auth
import pandas_gbq


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

def create_typology(df:pd.DataFrame)->pd.DataFrame:
    df2 = df.groupby(['Route Typology']).agg({"Route":"nunique"}).reset_index()
    df2 = df2.rename(columns = {"Route":"Total Routes"})
    return df2


def find_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bin 'Route Length Miles' into percentile categories and merge
    human-readable group labels. Zeros are labeled 'Zero'.
    """
    col = 'Route Length Miles'

    # Compute quartiles once
    p25, p50, p75 = df[col].quantile([0.25, 0.50, 0.75])

    # Build bins: (-inf, 0], (0, p25], (p25, p50], (p50, p75], (p75, inf)
    bins = [-np.inf, 0, p25, p50, p75, np.inf]
    labels = ['Zero', '25th percentile', '50th percentile', '< 75th percentile', '> 75th percentile']

    out = df.copy()
    out['percentile_cat'] = pd.cut(
        out[col],
        bins=bins,
        labels=labels,
        right=True,                # include upper bound in each interval
        include_lowest=True        # include lowest value
    )

    # Build concise label text using the computed thresholds
    percentile_df = pd.DataFrame({
        'percentile_cat': labels[1:],  # exclude 'Zero' from the mapping table
        'Route Length Miles Percentile Group': [
            f"25 percentile (<= {p25:.1f} miles)",
            f"26-50th percentile ({p25:.1f}-{p50:.1f} miles)",
            f"51-75th percentile ({p50:.1f}-{p75:.1f} miles)",
            f"76th percentile (>= {p75:.1f} miles)",
        ],
    })

    # Merge and drop 'Geometry' if present
    m1 = out.merge(percentile_df, on='percentile_cat', how='left')
    if 'Geometry' in m1.columns:
        m1 = m1.drop(columns=['Geometry'])

    return m1


def reshape_percentile_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Total number of routes by each
    the route_length_miles_percentile groups.
    """
    agg1 = (
        df.groupby(["Route Length Miles Percentile Group",])
        .agg({"Route Name": "nunique"})
        .reset_index()
    ).rename(
        columns={"Route Name": "Total Routes"}
    )
    return agg1

    
"""
Route Typology
"""   
def create_route_lengths(df: pd.DataFrame):
    df2 = find_percentiles(df)
    df3 = reshape_percentile_groups(df2)
    
    chart_dict = readable_dict.route_percentiles

    chart = _portfolio_charts.bar_chart(
    df = df3,
    x_col = "Route Length Miles Percentile Group",
    y_col = "Total Routes",
    color_col = "Route Length Miles Percentile Group",
    color_scheme = [*chart_dict.colors],
    tooltip_cols = list(chart_dict.tooltip),
    date_format = "",
    y_ticks = chart_dict.ticks,
)
    
    chart = (
        _portfolio_charts.configure_chart(
            chart,
            width=400,
            height=250,
            title=chart_dict.title,
            subtitle=chart_dict.subtitle,
        )
    )
    return chart

    
def create_route_typology(df: pd.DataFrame):
    typology_df = create_typology(df)
    chart_dict = readable_dict.route_typology

    chart = _portfolio_charts.pie_chart(df = typology_df,
         color_col = 'Route Typology',
         theta_col = 'Total Routes',
         color_scheme = [*chart_dict.colors],
         tooltip_cols = list(chart_dict.tooltip))
    
    chart = (
        _portfolio_charts.configure_chart(
            chart,
            width=200,
            height=250,
            title=chart_dict.title,
            subtitle="",
        )
    )
    return chart

"""
RT Data Charts
"""
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
    
    bg = _portfolio_charts.create_bg_service_chart()
    
    chart = (chart + bg).properties(
    resolve=alt.Resolve(
        scale=alt.LegendResolveMap(color=alt.ResolveMode("independent"))
    )
)
    chart = _portfolio_charts.configure_chart(
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


def text_table(df: pd.DataFrame) -> alt.Chart:

    # Create the chart
    text_chart = (
        alt.Chart(df)
        .mark_text()
        .encode(x=alt.X("Zero:Q", axis=None), y=alt.Y("combo_col", axis=None))
    )

    text_chart = text_chart.encode(text="combo_col:N")
    return text_chart

    
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

