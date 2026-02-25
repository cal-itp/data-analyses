"""
Chart and map functions for report.
"""

import _color_palette
import altair as alt
import pandas as pd

alt.data_transformers.enable("vegafusion")
alt.data_transformers.enable(consolidate_datasets=True)


def chart_ordered_by_stop(
    df: pd.DataFrame,
    stop_col: str = "stop_rank",
    y_col: str = "",
    direction_col: str = "direction_id",
    dropdown_selection: alt.Parameter = None,
    is_faceted: bool = False,
) -> alt.Chart:
    """ """
    Y_MIN_VALUE = df[y_col].min()
    if Y_MIN_VALUE <= 1:
        Y_MIN = round(Y_MIN_VALUE, 1) - 0.1
        Y_MAX = 1.1
    else:
        Y_MIN = round(Y_MIN_VALUE, -1) - 10
        Y_MAX = df[y_col].max() + 3

    chart = (
        alt.Chart(df)
        .encode(
            x=alt.X(f"{stop_col}:Q", title="Stop (ordered)"),
            y=alt.Y(f"{y_col}:Q", title="", scale=alt.Scale(domain=[Y_MIN, Y_MAX])),
            tooltip=["route_name", direction_col, "stop_id", "stop_name", stop_col, y_col],
            opacity=alt.when(dropdown_selection).then(alt.value(1)).otherwise(alt.value(0.2)),
            strokeWidth=alt.when(dropdown_selection).then(alt.value(2)).otherwise(alt.value(1)),
        )
        .add_params(dropdown_selection)
        .transform_filter(dropdown_selection)
        .interactive()
    )

    if is_faceted:
        chart = chart.encode(column=alt.Column(f"{direction_col}:Q", title="Direction"))

    return chart


def horiz_line_chart(df: pd.DataFrame, horiz_y_value: float) -> alt.Chart:
    """
    Add a dotted horizontal line to show where desired metric
    should be.

    Couldn't get alt.datum to work from this:
    https://altair-viz.github.io/user_guide/encodings/index.html#datum-and-value
    """
    df = df.assign(horiz_line=horiz_y_value)

    # rule = alt.Chart(df).mark_rule(strokeDash=[2, 2]).encode(
    #    y=alt.datum(0)
    # )
    rule = alt.Chart(df).mark_rule(color="gray", strokeWidth=1, strokeDash=[2, 2]).encode(y="horiz_line")

    return rule


def prediction_error_categories_stacked_bar(
    stop_df: pd.DataFrame,
    stop_col: str = "stop_id",
    category_col: str = "prediction_error_label",
    direction_col: str = "direction_id",
    dropdown_selection: alt.Parameter = None,
    legend_selection: alt.Parameter = None,
) -> alt.Chart:
    """ """
    # should this use aggregated stop_df?
    stop_df_grouped_by_route = (
        stop_df.groupby(["route_name", "direction_id", category_col], dropna=False)
        .agg({stop_col: "count"})
        .reset_index()
    )

    error_counts_by_route_stacked_bar = (
        alt.Chart(stop_df_grouped_by_route)
        .mark_bar()
        .encode(
            # get counts of stops by each error category summed up for route
            x=alt.X(
                category_col,
                title="Prediction Error",
                sort=list(_color_palette.PREDICTION_ERROR_COLOR_PALETTE.keys()),
                # adding scale to keep all categories is wonky with the bars shifting - need to add zeroes for this to work
                # scale=alt.Scale(domain=list(_color_palette.PREDICTION_ERROR_COLOR_PALETTE.keys()))
            ),
            y=alt.Y(f"sum({stop_col})", title="# stops"),
            color=alt.Color(
                category_col,
                sort=list(_color_palette.PREDICTION_ERROR_COLOR_PALETTE.keys()),
                scale=alt.Scale(range=list(_color_palette.PREDICTION_ERROR_COLOR_PALETTE.values())),
            ),
            column=alt.Column(direction_col, title="Direction"),
            tooltip=["route_name", direction_col, category_col, f"sum({stop_col})"],
            opacity=alt.when(dropdown_selection).then(alt.value(1)).otherwise(alt.value(0.2)),
            strokeWidth=alt.when(dropdown_selection).then(alt.value(2)).otherwise(alt.value(1)),
        )
        .transform_filter(dropdown_selection)
        .add_params(dropdown_selection, legend_selection)
        .interactive()
    )

    return error_counts_by_route_stacked_bar


def pct_completeness_line_chart(
    stop_df: pd.DataFrame,
    stop_col: str = "stop_rank",
    y_col: str = "pct_tu_complete_minutes",
    direction_col: str = "direction_id",
    dropdown_selection: alt.Parameter = None,
    horiz_y_value: float = 0.9,
) -> alt.Chart:
    """ """
    line_chart = (
        chart_ordered_by_stop(
            stop_df,
            stop_col=stop_col,
            y_col=y_col,
            direction_col=direction_col,
            dropdown_selection=dropdown_selection,
            is_faceted=False,
        )
        .mark_line()
        .encode(color=alt.value(_color_palette.get_color("lady_blue")))
    )

    horiz_line = horiz_line_chart(stop_df, horiz_y_value=horiz_y_value)

    point_chart = (
        chart_ordered_by_stop(
            stop_df,
            stop_col=stop_col,
            y_col=y_col,
            direction_col=direction_col,
            dropdown_selection=dropdown_selection,
            is_faceted=False,
        )
        .mark_point(size=10, strokeWidth=2)
        .encode(color=alt.value(_color_palette.get_color("lady_blue")))
    )

    chart = line_chart + point_chart + horiz_line

    return chart


def bus_catch_likelihood_line_chart(
    stop_df: pd.DataFrame,
    stop_col: str = "stop_rank",
    y_col: str = "bus_catch_likelihood",
    direction_col: str = "direction_id",
    dropdown_selection: alt.Parameter = None,
    horiz_y_value: float = 0.8,
):
    line_chart = (
        chart_ordered_by_stop(
            stop_df,
            stop_col=stop_col,
            y_col="bus_catch_likelihood",
            direction_col=direction_col,
            dropdown_selection=dropdown_selection,
            is_faceted=False,
        )
        .mark_line()
        .encode(color=alt.value(_color_palette.get_color("lizard_green")))
    )

    horiz_line = horiz_line_chart(stop_df, horiz_y_value=horiz_y_value)

    point_chart = (
        chart_ordered_by_stop(
            stop_df,
            stop_col=stop_col,
            y_col=y_col,
            direction_col=direction_col,
            dropdown_selection=dropdown_selection,
            is_faceted=False,
        )
        .mark_point(size=10, strokeWidth=2)
        .encode(color=alt.value(_color_palette.get_color("lizard_green")))
    )

    chart = line_chart + point_chart + horiz_line

    return chart


def prediction_spread_line_chart(
    stop_df: pd.DataFrame,
    stop_col: str = "stop_rank",
    y_col: str = "avg_prediction_spread_minutes",
    direction_col: str = "direction_id",
    dropdown_selection: alt.Parameter = None,
):
    line_chart = (
        chart_ordered_by_stop(
            stop_df,
            stop_col=stop_col,
            y_col=y_col,
            direction_col=direction_col,
            dropdown_selection=dropdown_selection,
            is_faceted=False,
        )
        .mark_line()
        .encode(color=alt.value(_color_palette.get_color("valentino")))
    )

    point_chart = (
        chart_ordered_by_stop(
            stop_df,
            stop_col=stop_col,
            y_col=y_col,
            direction_col=direction_col,
            dropdown_selection=dropdown_selection,
            is_faceted=False,
        )
        .mark_point(size=10, strokeWidth=2)
        .encode(color=alt.value(_color_palette.get_color("valentino")))
    )

    chart = line_chart + point_chart

    return chart


def fig5and6_prediction_error_plots(
    df: pd.DataFrame,
) -> alt.Chart:
    """
    Operator percentile plot
    """
    selection = alt.selection_point(fields=["service_date"], bind="legend")

    chart1 = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("negative_prediction_error_sec", title="Prediction Error (seconds)"),
            y=alt.Y("percentile", title="Percentiles", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color(
                "yearmonthdate(service_date):N",
                title="Date",
                scale=alt.Scale(
                    range=_color_palette.FULL_CATEGORICAL_COLORS
                    + _color_palette.TRI_COLORS
                    + _color_palette.FOUR_COLORS
                    + _color_palette.FOUR_COLORS2
                ),
            ),
            opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
            strokeWidth=alt.when(selection).then(alt.value(2)).otherwise(alt.value(1)),
            tooltip=["percentile", "negative_prediction_error_sec"],
        )
    )

    chart2 = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("positive_prediction_error_sec"),
            y=alt.Y("percentile"),
            color=alt.Color(
                "yearmonthdate(service_date):N",
                scale=alt.Scale(
                    range=_color_palette.FULL_CATEGORICAL_COLORS
                    + _color_palette.TRI_COLORS
                    + _color_palette.FOUR_COLORS
                    + _color_palette.FOUR_COLORS2
                ),
            ),
            opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
            strokeWidth=alt.when(selection).then(alt.value(2)).otherwise(alt.value(1)),
            tooltip=["percentile", "positive_prediction_error_sec"],
        )
    )

    vertical_line = alt.Chart().mark_rule(strokeDash=[12, 6], size=2, color="gray").encode(x=alt.datum(0))

    chart = (
        (chart1 + chart2 + vertical_line)
        .add_params(selection)
        .properties(
            title={
                "text": "Prediction Error Percentiles Plot",
                # "subtitle": f"{one_date}"
            }
        )
        .resolve_scale(y="shared")
        .interactive()
    )

    return chart
