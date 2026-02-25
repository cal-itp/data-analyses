"""
Data prep related to removing outliers.
Thresholds here are based on exploratory work
from 2 week June 2025 period to find 1% tails.
"""

import pandas as pd

MIN_ERROR_SEC_5MIN = -60 * 5
MAX_ERROR_SEC_5MIN = 60 * 5


def drop_outliers(df: pd.DataFrame, min_cutoff: int, max_cutoff: int) -> pd.DataFrame:
    """
    Drop outliers based on avg_prediction_error_sec for now.
    Use 250 seconds on either end, this is roughly 1% of either tail.
    In the future, we might expand to other columns.
    """
    return df[(df.avg_prediction_error_sec >= min_cutoff) & (df.avg_prediction_error_sec <= max_cutoff)].reset_index(
        drop=True
    )


def prediction_count_sanity_check(df: pd.DataFrame) -> pd.DataFrame:
    """
    Noticed that there are some rows where predictions (when aggregated
    to stop) do not line up to 100%.
    Why?
    - Does this have to do with losing stop_sequence (stop_time grain -> stop grain)?
    - Are these all on-time ones that fall between/exactly on "actual" arrival and departure?
    - small % of these, need to go back to dbt models to check
    """
    PREDICTION_CATEGORIES = ["early", "ontime", "late"]

    pct_cols = [f"pct_tu_predictions_{i}" for i in PREDICTION_CATEGORIES]
    count_cols = [f"n_predictions_{i}" for i in PREDICTION_CATEGORIES]

    df = df.assign(n_predictions2=df[count_cols].sum(axis=1), pct_predictions2=df[pct_cols].sum(axis=1))

    return df


def remove_rows_where_needing_investigation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perhaps some are due to rounding of rounding to 1 decimal place.
    Allow those to be pretty close to be included.

    Otherwise, for now, let's remove the ones where counts
    do not fall within the boundaries of rounding errors.
    """
    df2 = (
        df[(df.pct_predictions2 >= 0.98) & (df.pct_predictions2 <= 1.02)]
        .reset_index(drop=True)
        .drop(columns=["pct_predictions2", "n_predictions2"])
    )

    return df2


def remove_outliers(stop_df: pd.DataFrame) -> pd.DataFrame:
    """ """
    return (
        stop_df.pipe(prediction_count_sanity_check)
        .pipe(
            drop_outliers,
            MIN_ERROR_SEC_5MIN,
            MAX_ERROR_SEC_5MIN,
            # use 5 minute cutoffs, this is also < 1% on either tail
        )
        .pipe(remove_rows_where_needing_investigation)
    )
