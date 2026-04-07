"""
Chart and map functions for operator report.
"""

import _color_palette
import altair as alt
import pandas as pd
from great_tables import GT


def basic_percentiles_line_chart(
    df: pd.DataFrame, x_col: str, y_col: str = "percentile", color_col: str = "day_type"
) -> alt.Chart:
    """
    Percentiles line chart, replicating Newmark paper's combined figures 5 and 6.
    y-axis is 10, 20, ..., 100, deciles.
    x-axis is prediction error.

    Color this by day_type (if we show more than weekday).
    """
    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(x_col, title="Prediction Error (minutes)"),
            y=alt.Y("percentile", title="Percentiles", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color(
                f"{color_col}:N",
                title=f"{color_col.replace('_', ' ').title()}",
                sort=list(_color_palette.DAY_TYPE_COLOR_PALETTE.keys()),
                scale=alt.Scale(range=list(_color_palette.DAY_TYPE_COLOR_PALETTE.values())),
            ),
            tooltip=[x_col, "percentile", color_col],
        )
    )
    return chart


def fig5and6_prediction_error_plots(df: pd.DataFrame) -> alt.Chart:
    """
    Negative and positive prediction error plots are combined side-by-side as 1 chart.

    The two halves of the chart mirror each other, so that 10th percentile is closer
    to zero and 50th percentile is further out from zero.
    For positive prediction errors, the percentiles have to get reversed.
    This is handled in the warehouse table already because of how confusing it is.
    Instead of [10, 20, ....90] for percentiles, it should show [90, 80, ...10].
    """
    # Make legend selectable
    selection = alt.selection_point(fields=["day_type"], bind="legend")

    neg_errors_chart = basic_percentiles_line_chart(df, x_col="neg_prediction_error_minutes").encode(
        opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
        strokeWidth=alt.when(selection).then(alt.value(2)).otherwise(alt.value(1)),
    )

    pos_errors_chart = basic_percentiles_line_chart(df, x_col="pos_prediction_error_minutes").encode(
        opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
        strokeWidth=alt.when(selection).then(alt.value(2)).otherwise(alt.value(1)),
    )

    # Add vertical line where zero is
    vertical_line = alt.Chart().mark_rule(strokeDash=[12, 6], size=2, color="gray").encode(x=alt.datum(0))

    chart = (
        (neg_errors_chart + pos_errors_chart + vertical_line)
        .add_params(selection)
        .properties(title="Prediction Error Percentiles Plot")
        .resolve_scale(y="shared")
        .interactive()
    )

    return chart


def format_great_table(table: GT, day_type_grouping: bool = False) -> GT:
    """
    Apply common table formatting - font size, center align,
    force table to span 100%, rather than shrinking
    when there are fewer columns to show. Easier to apply column % width this way.

    Sometimes we want to group by day_type.
    https://posit-dev.github.io/great-tables/reference/GT.tab_stub.html
    """
    formatted_table = table.tab_options(table_font_size="11px", table_width="100%").cols_align("center")

    if day_type_grouping:
        formatted_table = formatted_table.tab_stub(rowname_col="day_type")

    return formatted_table
