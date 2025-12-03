"""
Data prep for operator report (data product #2).
Mostly trip updates, but some vp and schedule metrics are also available.
"""

import pandas as pd


def add_slope_of_prediction_error(
    df: pd.DataFrame, group_cols: list = ["schedule_name", "service_date"], prediction_error_col: str = ""
) -> pd.DataFrame:
    """
    Make sure the slope is calculated for all the dates present.
    """
    p10_numerator = df[df.percentile == 10][group_cols + [prediction_error_col]].rename(
        columns={prediction_error_col: "p10"}
    )

    p50_denominator = df[df.percentile == 50][group_cols + [prediction_error_col]].rename(
        columns={prediction_error_col: "p50"}
    )

    slope_df = pd.merge(p10_numerator, p50_denominator, on=group_cols)

    slope_df[f"{prediction_error_col}_slope"] = slope_df.p10 / slope_df.p50

    df2 = pd.merge(df, slope_df[group_cols + [f"{prediction_error_col}_slope"]], on=group_cols)

    return df2


def explode_decile_array_to_long(df: pd.DataFrame):
    """
    Get an exploded, cleaned up version of these deciles calculated for
    positive and negative prediction errors.
    Clean up column names so it's easier to use in charts and tables.
    """
    positive_error_cols = ["positive_prediction_error_sec_array", "positive_percentile_array"]
    negative_error_cols = ["negative_prediction_error_sec_array", "negative_percentile_array"]

    group_cols = ["schedule_name", "service_date"]

    positive_df = (
        df[group_cols + positive_error_cols]
        .explode(column=positive_error_cols)
        .rename(
            columns={
                "positive_percentile_array": "percentile",
                "positive_prediction_error_sec_array": "positive_prediction_error_sec",
            }
        )
        .pipe(add_slope_of_prediction_error, group_cols, "positive_prediction_error_sec")
    )

    negative_df = (
        df[group_cols + negative_error_cols]
        .explode(column=negative_error_cols)
        .rename(
            columns={
                "negative_percentile_array": "percentile",
                "negative_prediction_error_sec_array": "negative_prediction_error_sec",
            }
        )
        .pipe(add_slope_of_prediction_error, group_cols, "negative_prediction_error_sec")
    )

    df2 = pd.merge(positive_df, negative_df, on=group_cols + ["percentile"])

    return df2


def prep_trip_updates_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    * For the positive and negative prediction errors, display 10th and 50th percentiles.
    * Calculate the slope (10th / 50th), because smaller the value, the smaller the loss in accuracy.
    * Prediction padding is absolute value of 5th percentile (not reversed)...is this 95th percentile then?!
       * TODO: reread paper, the few lines written on this is fairly confusing
       * reversed percentiles show outliers at the bottom of graphs (Fig 5, 6)
       * so not reversed means...what?
       * If this is the padding to catch the bus, then presumably it means it's attached to late predictions,
         because following those would mean you miss the bus, and behaviorally, riders take into account this handicap and adjust
    * Bus catch likelihood is pct_predictions_early + pct_predictions_ontime, also show n_predictions
    """
    keep_cols = [
        "schedule_name",
        "service_date",
        "n_predictions",
        "pct_predictions_early",
        "pct_predictions_ontime",
        "pct_tu_complete_minutes",
        "pct_tu_accurate_minutes",
        "avg_prediction_spread_minutes",
        "tu_messages_per_minute",
        "prediction_error_sec_p10",
        "prediction_error_sec_p25",
        "prediction_error_sec_p50",
        "prediction_error_sec_p75",
        "prediction_error_sec_p90",
        "pct_tu_trips",
    ]

    df2 = (
        df[keep_cols]
        .assign(
            bus_catch_likelihood=(df.pct_predictions_early + df.pct_predictions_ontime).round(2),
            prediction_padding_minutes=df.prediction_error_sec_p95.divide(60).round(1),
            prediction_error_minutes_iqr=(df.prediction_error_sec_p75 - df.prediction_error_sec_p25)
            .divide(60)
            .round(1),
            prediction_error_minutes_p50=df.prediction_error_sec_p50.divide(60).round(1),
        )
        .sort_values(["schedule_name", "service_date"])
        .reset_index(drop=True)
    )

    return df2
