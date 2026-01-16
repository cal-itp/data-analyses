import altair as alt
import geopandas as gpd
import great_tables as gt
import pandas as pd
from calitp_data_analysis import calitp_color_palette as cp


def labeling(word: str) -> str:
    return (
        word.replace("_", " ")
        .title()
        .replace("N", "Total")
        .replace("Pct", "%")
        .replace("Vp", "VP")
    )

blue_palette = ["#B9D6DF", "#2EA8CE", "#0B405B"]

def grouped_bar_chart(
    df: pd.DataFrame,
    color_col: str,
    y_col: str,
    offset_col: str,
    title: str,
    subtitle: str,
):
    df = df.assign(
        time_period=df.time_period.str.replace("_", " ").str.title()
    ).reset_index(drop=True)

    df[y_col] = df[y_col].fillna(0).astype(int)
    tooltip_cols = [
        "direction_id",
        "time_period",
        "route_combined_name",
        "organization_name",
        "service_date",
        y_col,
    ]
    """ 
    ruler = (
        alt.Chart(df)
        .mark_rule(color="red", strokeDash=[10, 7])
        .encode(y=f"mean({y_col}):Q")
    )
    """
    chart = (
        alt.Chart(df)
        .mark_bar(size=10)
        .encode(
            x=alt.X(
                "yearmonthdate(service_date):O",
                title=["Grouped by Direction ID", "Date"],
                axis=alt.Axis(format="%b %Y"),
            ),
            y=alt.Y(f"{y_col}:Q", title=labeling(y_col)),
            xOffset=alt.X(f"{offset_col}:N", title=labeling(offset_col)),
            color=alt.Color(
                f"{color_col}:N",
                title=labeling(color_col),
                scale=alt.Scale(
                    range=blue_palette,
                ),
            ),
            tooltip=tooltip_cols,
        )
    )
    chart = (chart).properties(
        title={
            "text": [title],
            "subtitle": [subtitle],
        },
        width=500,
        height=300,
    )

    return chart

def heatmap(
    df: pd.DataFrame,
    color_col: str,
    title: str,
    subtitle1: str,
    subtitle2: str,
    subtitle3: str,
):
    df = df.assign(
        time_period=df.time_period.str.replace("_", " ").str.title()
    ).reset_index(drop=True)

    # Grab original column that wasn't categorized
    original_col = color_col.replace("_cat", "")

    tooltip_cols = [
        "direction_id",
        "time_period",
        "route_combined_name",
        "organization_name",
        color_col,
        original_col,
    ]

    # Round
    # df[color_col] = df[color_col].round(1)
    chart = (
        alt.Chart(df)
        .mark_rect(size=30)
        .encode(
            x=alt.X(
                "yearmonthdate(service_date):O",
                axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                title=["Grouped by Direction ID", "Service Date"],
            ),
            y=alt.Y("time_period:O", title=["Time Period"]),
            xOffset=alt.X(f"direction_id:N", title="Direction ID"),
            color=alt.Color(
                f"{color_col}:N",
                title=labeling(color_col),
                scale=alt.Scale(range=cp.CALITP_SEQUENTIAL_COLORS),
            ),
            tooltip=tooltip_cols,
        )
        .properties(
            title={"text": [title], "subtitle": [subtitle1, subtitle2, subtitle3]},
            width=500,
            height=300,
        )
    )

    text = chart.mark_text(baseline="middle").encode(
        alt.Text(original_col), color=alt.value("white")
    )

    final_chart = chart + text
    return final_chart