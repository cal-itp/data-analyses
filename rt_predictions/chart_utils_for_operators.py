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
    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(x_col, title="Prediction Error (minutes)"),
            y=alt.Y("percentile", title="Percentiles", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color(
                f"{color_col}:N",
                sort=list(_color_palette.DAY_TYPE_COLOR_PALETTE.keys()),
                scale=alt.Scale(range=list(_color_palette.DAY_TYPE_COLOR_PALETTE.values())),
            ),
            tooltip=[x_col, "percentile", color_col],
        )
    )
    return chart


def fig5and6_prediction_error_plots(df: pd.DataFrame) -> alt.Chart:
    """ """
    selection = alt.selection_point(fields=["day_type"], bind="legend")

    neg_errors_chart = basic_percentiles_line_chart(df, x_col="neg_prediction_error_minutes").encode(
        opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
        strokeWidth=alt.when(selection).then(alt.value(2)).otherwise(alt.value(1)),
    )

    pos_errors_chart = basic_percentiles_line_chart(df, x_col="pos_prediction_error_minutes").encode(
        opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
        strokeWidth=alt.when(selection).then(alt.value(2)).otherwise(alt.value(1)),
    )

    vertical_line = alt.Chart().mark_rule(strokeDash=[12, 6], size=2, color="gray").encode(x=alt.datum(0))

    chart = (
        (neg_errors_chart + pos_errors_chart + vertical_line)
        .add_params(selection)
        .properties(title="Prediction Error Percentiles Plot")
        .resolve_scale(y="shared")
        .interactive()
    )

    return chart


def format_great_table(table: GT) -> GT:
    """ """
    formatted_table = (
        table.tab_options(table_font_size="11px", table_width="100%")
        .tab_stub(rowname_col="day_type")
        .cols_align("center")
    )
    return formatted_table
