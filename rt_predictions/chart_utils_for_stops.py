"""
Chart and map functions for report.
"""

import _color_palette
import altair as alt
import folium
import geopandas as gpd
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


def boxplot_by_date(df: pd.DataFrame, y_col: str) -> alt.Chart:
    """
    Get a boxplot for each day to look at distribution avg_prediction_error_minutes
    and avg_prediction_spread_minutes.
    These are more suited to see how "early" or "late" an operator's predictions are.
    If it's centered at 0, that's very on-time/accurate!

    If x-axis (day_type) isn't quantitative, this chart doesn't become interactive.
    https://github.com/vega/altair/issues/933
    Workaround: make it quantitative, then assign the labels we want.
    https://stackoverflow.com/questions/68841230/how-to-replace-the-axis-label-in-altair
    """
    DAYTYPE_ORDER_DICT = {"Weekday": 1, "Saturday": 2, "Sunday": 3}

    day_type_axis_labels = (
        "datum.label == 1 ? 'Weekday' : datum.label == 2 ? 'Saturday' : datum.label == 3 ? 'Sunday' : ''"
    )

    df = df.assign(day_type_ordered=df.day_type.map(DAYTYPE_ORDER_DICT))

    Y_TITLECASE = f"{y_col.replace('_', ' ').title()}"
    chart = (
        alt.Chart(df)
        .mark_boxplot()
        .encode(
            x=alt.X(
                "day_type_ordered:Q",
                axis=alt.Axis(values=[1, 2, 3], labelExpr=day_type_axis_labels),
            ),
            y=alt.Y(f"{y_col}:Q", title=Y_TITLECASE).scale(zero=True),
            color=alt.Color(
                "day_type:N",
                scale=alt.Scale(
                    domain=["Weekday", "Saturday", "Sunday"],
                    range=_color_palette.TRI_COLORS,
                ),
            ),
        )
    )

    rule = alt.Chart(df).mark_rule(strokeWidth=1, color="black", strokeDash=[2, 2]).encode(y=alt.datum(0))

    combined = (chart + rule).properties(title=Y_TITLECASE)

    return combined


def bar_chart_by_date(df: pd.DataFrame, legend_color_column: str, is_stacked: bool) -> alt.Chart:
    selection = alt.selection_point(fields=[legend_color_column], bind="legend")

    chart = (
        alt.Chart(df)
        .mark_bar(size=20)
        .encode(
            x=alt.X("service_date:T", axis=alt.Axis(format="%b %e")),
            y=alt.Y("count()"),
            color=alt.Color(
                f"{legend_color_column}:N",
                sort=[
                    "5+ min early",
                    "3-5 min early",
                    "1-3 min early",
                    "1 min early to 1 min late",
                    "1-3 min late",
                    "5+ min late",
                    "unknown",
                ],
                scale=alt.Scale(range=_color_palette.FULL_CATEGORICAL_COLORS),
            ),
            opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
            tooltip=["service_date", legend_color_column, "count()"],
        )
        .add_params(selection)
        .properties(
            title=f"{legend_color_column.replace('_', ' ').replace('label', '').title()}", width=350, height=300
        )
        .interactive()
    )

    if is_stacked:
        chart = chart.encode(y=alt.Y("count()", stack="normalize"))

    return chart


def plot_basic_map(gdf: gpd.GeoDataFrame, plot_col: str, colorscale: str):
    """
    Function for map arguments.
    """
    m = gdf.explore(
        plot_col,
        tiles="CartoDB Positron",
        cmap=colorscale,
        legend=True,
        legend_kwds={"caption": f"{plot_col.replace('pct_tu', '%').replace('_', ' ').title()}"},
    )

    return m


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


def stacked_bar_chart_by_route(
    df: pd.DataFrame,
    x_col: str = "prediction_error_label",
    y_col: str = "route_name",
    color_col: str = "prediction_error_label",
) -> alt.Chart:
    """
    Stacked bar chart showing how many stops were in
    each prediction_error category by route-direction.
    By day_type or across all day_types.

    TODO: title can be named better, use dict to change column names to title
    """
    selection = alt.selection_point(fields=[x_col], bind="legend")
    SORT_DICT = {
        "prediction_error_label": [
            "3-5 min early",
            "1-3 min early",
            "1 min early to 1 min late",
            "1-3 min late",
            "3-5 min late",
        ],
        "n_stops": [
            "3-5 min early",
            "1-3 min early",
            "1 min early to 1 min late",
            "1-3 min late",
            "3-5 min late",
        ],
    }

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"sum({x_col})", title="count"),
            y=alt.Y(
                f"{y_col}:N",
                title="Route",
            ),
            color=alt.Color(
                color_col, sort=SORT_DICT[x_col], scale=alt.Scale(range=_color_palette.FULL_CATEGORICAL_COLORS)
            ),
            column=alt.Column("direction_id", title="Direction"),
            tooltip=["schedule_name", "day_type", x_col, y_col, "direction_id", color_col],
            opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
            strokeWidth=alt.when(selection).then(alt.value(2)).otherwise(alt.value(1)),
        )
        .add_params(selection)
        .properties(title=f"{x_col} by Route")
        .interactive()
    )

    return chart


def make_layer_map(
    gdf: gpd.GeoDataFrame, plot_col: str, layer_col: str = "route_name", sort_layer_col: str = "route_name"
):
    # https://github.com/python-visualization/folium/issues/1857
    # couldn't get choropleth and geojson to work, this one works better
    # https://stackoverflow.com/questions/75398354/show-multiple-layers-on-geopandas-explore-with-correct-legend-labels
    sorted_routes = sorted(gdf[sort_layer_col].unique().tolist())

    first_layer_name = sorted_routes[0]
    subset_gdf = gdf[gdf[sort_layer_col] == first_layer_name].reset_index(drop=True)

    m = subset_gdf.explore(
        plot_col,
        tiles="CartoDB Positron",
        name=subset_gdf[layer_col].iloc[0],
    )

    for one_route_layer in sorted_routes[1:]:
        subset_gdf = gdf[gdf[sort_layer_col] == one_route_layer].reset_index(drop=True)
        m = subset_gdf.explore(plot_col, m=m, name=subset_gdf[layer_col].iloc[0], legend=False, control=True, show=True)
    folium.LayerControl().add_to(m)

    return m


def stripplot_by_route(df: pd.DataFrame, plot_col: str = "pct_tu_complete_minutes"):
    # update availability by route
    stop_route_cols_tabular = ["schedule_name", "day_type", "stop_id", "route_id", "direction_id"]

    plot_col = "pct_tu_complete_minutes"

    df2 = (
        df[stop_route_cols_tabular + [plot_col]]
        .groupby(["schedule_name", "day_type", "route_id", "direction_id"], dropna=False)
        .agg({"pct_tu_complete_minutes": "mean", "stop_id": "count"})
        .reset_index()
        .rename(columns={"stop_id": "n_stops"})
    )

    df2 = df2.assign(pct_tu_complete_minutes=df2.pct_tu_complete_minutes.round(3) * 100)

    # 89 rounds to 90, then set axis to 80
    Y_MIN = round(df2[plot_col].min(), -1) - 10

    chart = (
        alt.Chart(df2)
        .mark_point(size=10)
        .encode(
            x=alt.X("day_type:N", sort=["Weekday", "Saturday", "Sunday"]),
            y=alt.Y(f"{plot_col}:Q", scale=alt.Scale(domain=[Y_MIN, 100])),
            color=alt.Color("day_type:N", sort=["Weekday", "Saturday", "Sunday"]),
            # size="n_stops",
            xOffset="jitter:Q",
            tooltip=["schedule_name", "route_id", "direction_id", "n_stops", plot_col],
        )
        .transform_calculate(
            # Generate Gaussian jitter with a Box-Muller transform
            jitter="sqrt(-2*log(random()))*cos(2*PI*random())"
        )
        .properties(width=300)
        .interactive()
    )

    return chart


def ranged_dot_plot(df: pd.DataFrame, x_col: str, y_col: str, ptile_col: str, title: str) -> alt.Chart:
    """
    Use this for IQR plot by route.
    https://altair-viz.github.io/gallery/ranged_dot_plot.html
    """
    chart = alt.Chart(df).encode(x=x_col, y=f"{y_col}:N", tooltip=[x_col, y_col])

    line = chart.mark_line(color="#db646f").encode(detail=f"{y_col}:N")

    # Add points for endpoints
    ptiles_to_plot = df[ptile_col].unique().tolist()
    color = alt.Color(f"{ptile_col}:O").scale(domain=ptiles_to_plot, range=["#e6959c", "#911a24"])
    points = (
        chart.mark_point(
            size=100,
            opacity=1,
            filled=True,
        )
        .encode(color=color)
        .interactive()
    )

    dot_chart = (line + points).properties(title=title)
    return dot_chart


def prediction_padding(prediction_padding_df):
    # can this one add selector for day_type?
    # then hide legend for route?
    chart = (
        alt.Chart(prediction_padding_df)
        .mark_point()
        .encode(
            x="prediction_error_minutes_array",
            y="scaled_prediction_error_sec_array",
            # column=alt.Column("day_type"),
            color=alt.Color("route_dir_name:N", legend=None),
            tooltip=[
                "prediction_error_minutes_array",
                "scaled_prediction_error_sec_array",
                "route_dir_name",
                "day_type",
            ],
        )
        .interactive()
        .properties(title="Prediction Padding")
    )
    return chart
