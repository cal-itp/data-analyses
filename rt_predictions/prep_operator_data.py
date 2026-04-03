"""
Data prep for operator report (data product #2).
Mostly trip updates, but some vp and schedule metrics are also available.
"""

import pandas as pd
import report_utils


def operator_percentiles_summary(
    df: pd.DataFrame, group_cols: list = ["month_first_day", "day_type", "schedule_name", "tu_name"]
) -> pd.DataFrame:
    """
    Several percentiles are linked to specific interpretations.
    There are 3 arrays (full, positive only, negative only), so combine the
    ones that are relevant into a df.

    - Prediction padding (5th percentile) on prediction_error_sec_percentile_array.
    - 25th, 50th, 75th (IQR + median) percentile on prediction_error_sec_percentile_array.
    - deciles to plot on pos_prediction_error_sec_array and neg_prediction_error_sec_array
    - drive ratio of 10th / 50th as accuracy loss on
      pos_prediction_error_sec_array and neg_prediction_error_sec_array.
    """
    PTILES_FOR_FULL_ARRAY = [5, 25, 50, 75]
    PTILES_FOR_SLOPE = [10, 50]

    percentiles_iqr = (
        report_utils.explode_percentiles(
            df,
            group_cols,
            array_col="prediction_error_sec_array",
            ptile_array_col="prediction_error_sec_percentile_array",
            ptiles_to_keep=PTILES_FOR_FULL_ARRAY,
        )
        .rename(columns={"p5": "prediction_padding"})
        .pipe(report_utils.convert_seconds_to_minutes, "p25")
        .pipe(report_utils.convert_seconds_to_minutes, "p75")
        .pipe(report_utils.convert_seconds_to_minutes, "prediction_padding")
        .pipe(report_utils.convert_seconds_to_minutes, "iqr")
    )

    pos_slope = (
        report_utils.explode_percentiles(
            df,
            group_cols,
            array_col="pos_prediction_error_sec_array",
            ptile_array_col="pos_prediction_error_sec_percentile_array",
            ptiles_to_keep=PTILES_FOR_SLOPE,
        )
        .rename(
            columns={
                **{f"p{i}": f"pos_p{i}" for i in PTILES_FOR_SLOPE},
            }
        )
        .reset_index(drop=True)
    )

    neg_slope = (
        report_utils.explode_percentiles(
            df,
            group_cols,
            array_col="neg_prediction_error_sec_array",
            ptile_array_col="prediction_error_sec_percentile_array",
            ptiles_to_keep=PTILES_FOR_SLOPE,
        )
        .rename(
            columns={
                **{f"p{i}": f"neg_p{i}" for i in PTILES_FOR_SLOPE},
            }
        )
        .reset_index(drop=True)
    )

    percentiles_df = pd.merge(percentiles_iqr, pos_slope, on=group_cols, how="inner").merge(
        neg_slope, on=group_cols, how="inner"
    )

    # now calculate slope
    # small is good, large ratios are bad (ex in paper is that 4 is pretty bad)
    # prediction_padding is always absolute value
    percentiles_df = percentiles_df.assign(
        pos_error_ratio=percentiles_df.pos_p10.divide(percentiles_df.pos_p50).round(1),
        neg_error_ratio=percentiles_df.neg_p10.divide(percentiles_df.neg_p50).round(1),
        prediction_padding=percentiles_df.prediction_padding.abs(),
    )

    return percentiles_df


def operator_deciles_for_chart(
    df: pd.DataFrame, group_cols: list = ["month_first_day", "day_type", "schedule_name", "tu_name"]
) -> pd.DataFrame:
    """
    Several percentiles are linked to specific interpretations.
    There are 3 arrays (full, positive only, negative only), so combine the
    ones that are relevant into a df.

    - Prediction padding (5th percentile) on prediction_error_sec_percentile_array.
    - 25th, 50th, 75th (IQR + median) percentile on prediction_error_sec_percentile_array.
    - deciles to plot on pos_prediction_error_sec_array and neg_prediction_error_sec_array
    - drive ratio of 10th / 50th as accuracy loss on
      pos_prediction_error_sec_array and neg_prediction_error_sec_array.
    """
    DECILES = list(range(0, 100, 10))
    pos_deciles = (
        report_utils.explode_percentiles(
            df,
            group_cols,
            array_col="pos_prediction_error_sec_array",
            ptile_array_col="pos_prediction_error_sec_percentile_array",
            ptiles_to_keep=DECILES,
            pivoted=False,  # keep long so we can make a chart!
        )
        .rename(
            columns={
                "pos_prediction_error_sec_percentile_array": "percentile",
                "pos_prediction_error_sec_array": "pos_prediction_error_sec",
            }
        )
        .pipe(report_utils.convert_seconds_to_minutes, "pos_prediction_error_sec")
    )

    neg_deciles = (
        report_utils.explode_percentiles(
            df,
            group_cols,
            array_col="neg_prediction_error_sec_array",
            ptile_array_col="prediction_error_sec_percentile_array",
            ptiles_to_keep=DECILES,
            pivoted=False,
        )
        .rename(
            columns={
                "prediction_error_sec_percentile_array": "percentile",
                "neg_prediction_error_sec_array": "neg_prediction_error_sec",
            }
        )
        .pipe(report_utils.convert_seconds_to_minutes, "neg_prediction_error_sec")
    )

    deciles_df = pd.merge(pos_deciles, neg_deciles, on=group_cols + ["percentile"], how="inner")

    return deciles_df


def merge_in_operator_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    There's a part we can import, then we need to calculate the percentiles we need
    to display in tables, and drop the arrays.
    """
    percentiles_df = operator_percentiles_summary(df)

    df1 = pd.merge(df, percentiles_df, on=["month_first_day", "day_type", "schedule_name", "tu_name"], how="inner")

    # can drop array cols
    # add a bit more
    array_cols = [c for c in df1.columns if "_array" in c]

    df1 = (
        df1.assign(
            bus_catch_likelihood=(df1.pct_predictions_early + df1.pct_predictions_ontime).round(2),
            day_type_sorted=df1.day_type.map(report_utils.DAYTYPE_ORDER_DICT),
        )
        .rename(columns={"prediction_padding": "prediction_padding_minutes"})
        .drop(columns=["pct_predictions_early", "pct_predictions_ontime"] + array_cols)
        .sort_values(["month_first_day", "schedule_name", "tu_name", "day_type_sorted"])
        .reset_index(drop=True)
    )

    return df1
