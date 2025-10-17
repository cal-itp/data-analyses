"""
Data prep related to removing outliers, adding new columns, rounding values,
renaming columns. 
Minimal data processing, move as much into dbt models, but
what shouldn't be done there should be handled here.
"""
import datetime
import geopandas as gpd
import pandas as pd
import google.auth
import sys

from loguru import logger

from rt_msa_utils import PREDICTIONS_GCS, RT_MSA_DICT

credentials, project = google.auth.default()

PREDICTION_CATEGORIES = ["early", "ontime", "late"]

MIN_ERROR_SEC = -250
MAX_ERROR_SEC = 250
MIN_ERROR_SEC_5MIN = -60 * 5
MAX_ERROR_SEC_5MIN = 60 * 5

PREDICTION_ERROR_COLS = [
    "avg_prediction_error_sec", 
    "pct_tu_predictions_early", "pct_tu_predictions_ontime", "pct_tu_predictions_late"
]

ACCURACY_COLS = [
    "pct_tu_accurate_minutes", "pct_tu_predictions_ontime"
]

# Drop a bunch of stuff for data wrangling we want to do for analysis or exploration
PERCENTILE_LIST = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

def drop_outliers(
    df: pd.DataFrame,
    min_cutoff: int,
    max_cutoff: int
) -> pd.DataFrame:
    """
    Drop outliers based on avg_prediction_error_sec for now.
    Use 250 seconds on either end, this is roughly 1% of either tail.
    In the future, we might expand to other columns.
    """
    return df[
        (df.avg_prediction_error_sec >= min_cutoff) & 
        (df.avg_prediction_error_sec <= max_cutoff)
    ].reset_index(drop=True)
    
    
def round_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rounding numeric columns, convert seconds to minutes for charts.
    TODO: added rounding to dbt models, those have been rounded to 2 decimal places, we
    want 3 decimal places
    """
    df = df.assign(
        avg_prediction_error_minutes = df.avg_prediction_error_sec.divide(
            60).round(3),
        avg_prediction_spread_minutes = df.avg_prediction_spread_minutes.round(2),
    ) 
    return df


def calculate_percents(df: pd.DataFrame):
    """
    Calculate percents for the daily stop df.
    In the aggregated one, these will be pre-calculated in dbt model.
    """
    df = df.assign(
        pct_tu_predictions_early = df.n_predictions_early.divide(
            df.n_predictions).round(3),
        pct_tu_predictions_ontime = df.n_predictions_ontime.divide(
            df.n_predictions).round(3),       
        pct_tu_predictions_late = df.n_predictions_late.divide(
            df.n_predictions).round(3),
        pct_tu_accurate_minutes = df.n_tu_accurate_minutes.divide(
            df.n_tu_minutes_available).round(3),
        pct_tu_complete_minutes = df.n_tu_complete_minutes.divide(
            df.n_tu_minutes_available).round(3),
        avg_predictions_per_trip = df.n_predictions.divide(
            df.n_tu_trips).round(3),
    ) 

    return df


def prediction_count_sanity_check(df: pd.DataFrame) -> pd.DataFrame:
    """
    Noticed that there are some rows where predictions (when aggregated 
    to stop) do not line up to 100%.
    Why? 
    - Does this have to do with losing stop_sequence (stop_time grain -> stop grain)?
    - Are these all on-time ones that fall between/exactly on "actual" arrival and departure?
    - small % of these, need to go back to dbt models to check
    """ 
    pct_cols = [
        f"pct_tu_predictions_{i}" for i in PREDICTION_CATEGORIES
    ]
    count_cols = [f"n_predictions_{i}" for i in PREDICTION_CATEGORIES]
    
    df = df.assign(
        n_predictions2 = df[count_cols].sum(axis=1),
        pct_predictions2 = df[pct_cols].sum(axis=1)
    )
    
    return df


def remove_rows_where_needing_investigation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perhaps some are due to rounding of rounding to 1 decimal place.
    Allow those to be pretty close to be included.
    
    Otherwise, for now, let's remove the ones where counts 
    do not fall within the boundaries of rounding errors.
    """
    df2 = df[
        (df.pct_predictions2 >= 0.98) & 
        (df.pct_predictions2 <= 1.02 )
    ].reset_index(drop=True).drop(
        columns = ["pct_predictions2", "n_predictions2"]
    )

    return df2


def pct_rows_need_investigation(df: pd.DataFrame):
    """
    Quick descriptive to understand how many rows are off, 
    even after accounting for rounding errors.
    """
    denominator = len(df)
    numerator = len(df[(df.pct_predictions2 >= 0.98) & (df.pct_predictions2 <= 1.02 )])

    # account for rounding error
    print(numerator/denominator)
    return


def categorize_day_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    This is already done for aggregated dfs.
    Add this so we can color altair charts more easily.
    """
    category_dict = {
        **{k: "Weekday" for k in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]},
        **{k: k for k in ["Saturday", "Sunday"]},
    }
    
    df = df.assign(
        day_type = df.service_date.dt.day_name().map(category_dict)
    )
    
    return df

def import_stop_df(
    is_daily: bool,
    filters: tuple,
) -> gpd.GeoDataFrame:
    """
    """
    if is_daily:
        subset_operators = pd.read_parquet(
            f"{PREDICTIONS_GCS}{RT_MSA_DICT.rt_schedule_models.weekday_stop_grain}.parquet",
            columns = ["tu_base64_url", "schedule_name"],
            filters = filters
        ).drop_duplicates().rename(
            columns = {"tu_base64_url": "base64_url"}
        )
            
        df = pd.read_parquet(
            f"{PREDICTIONS_GCS}{RT_MSA_DICT.rt_trip_updates_downloads.daily_stop_grain}.parquet",
            filters = [[("base64_url", "in", subset_operators.base64_url.tolist())]]
        ).merge(
            subset_operators,
            on = "base64_url",
            how = "inner"
        ).pipe(
            calculate_percents
        ).pipe(
            round_columns
        ).pipe(
            prediction_count_sanity_check
        ).pipe(
            drop_outliers,
            MIN_ERROR_SEC, MAX_ERROR_SEC
        ).pipe(
            remove_rows_where_needing_investigation
        ).pipe(
            categorize_day_type
        ).pipe(
            categorize_prediction_error, 
            "avg_prediction_error_minutes"
        )


    else:
        df = gpd.read_parquet(
            f"{PREDICTIONS_GCS}{RT_MSA_DICT.rt_schedule_models.weekday_stop_grain}.parquet",
            filters = filters,
            storage_options={"token": credentials.token}
        ).pipe(
            round_columns
        ).pipe(
            prediction_count_sanity_check
        ).pipe(
            drop_outliers,
            MIN_ERROR_SEC_5MIN, MAX_ERROR_SEC_5MIN 
            # use 5 minute cutoffs, this is also < 1% on either tail
        ).pipe(
            remove_rows_where_needing_investigation
        ).pipe(
            categorize_prediction_error,
            "avg_prediction_error_minutes"
        )
    
    return df


def daily_binned_counts_by_deciles(
    df: pd.DataFrame, 
    percentile_columns: list
):
    """
    Create side-by-side chart that will show distributions (deciles) for 
    pct_tu_predictions_early/ontime/late (compare daily vs daytype aggregation).
    Might need to pre-aggregate or the notebook will not produce portfolio for
    larger operators (4 sec timeout for autosave).
    """
    bins = pd.IntervalIndex.from_tuples([
        (0, 0.1), (0.101, 0.2), (0.201, 0.3),
        (0.301, 0.4), (0.401, 0.5), (0.501, 0.6),
        (0.601, 0.7), (0.701, 0.8), (0.801, 0.9), (0.901, 1.0)
    ])
        
    for c in percentile_columns:
        # For a column, categorize it into a decile
        df[f"{c}_bin"] = pd.cut(df[c], bins)
        
    return df


def summary_counts_by_bins(
    df: pd.DataFrame, 
    group_cols: list,
    percentile_columns: list
):
    """
    Get daily counts for each decile bin.
    These will be concatenated into a long df so that the altair line chart
    can share the same x-axis (decile bins) and legend can select a 
    particular service_date to highlight.
    
    df coming in looks like:
        stop1  decile1
        stop2  decile1
        stop3  decile2
    
    post aggregation df:
        decile1  2 stops  metric1
        decile2  1 stop   metric1
    """
    summary_dfs = []
    
    for col in percentile_columns:
    
        binned_column = f"{col}_bin"

        df2 = (
            df
            .groupby(group_cols + [binned_column])
            .agg({"stop_key": "count"})
            .reset_index()
            .rename(columns = {
                binned_column: "decile_bin",
                "stop_key": "counts"
            })
        )

        df2 = df2.assign(
            metric = col
        )
        
        summary_dfs.append(df2)
    
    
    # Concatenate and create a long df, where each row is service_date-operator-decile-metric.
    # day1-operator-1st_decile-pct_early, day1-operator-1st_decile-pct_ontime      
    summary_df = pd.concat(
        summary_dfs,
        axis=0, ignore_index=True
    ).astype(
        {"decile_bin": "str"}
    )

    return summary_df    


def categorize_prediction_error(
    df: pd.DataFrame, 
    prediction_error_col: str
) -> pd.DataFrame:
    """
    Using exponential equation to determine accuracy means it is
    fairly assertive.
    However, we can convert that to minutes and get a better understanding
    of how far out operators are doing.
    """
    UPPER = 5  # 5 minutes
    MIDDLE = 3 
    LOWER = 1
    
    PREDICTION_ERROR_LABELS = {
        "very_early": "5+ min early", 
        "early": "3-5 min early",
        "little_early": "1-3 min early",
        "ontime": "1 min early to 1 min late", 
        "little_late": "1-3 min late", 
        "late": "3-5 min late",
        "very_late": "5+ min late",
        "unknown": "unknown"
    }
    
    df["prediction_error_category"] = df.apply(
        lambda x: set_error_cutoffs(
            x[prediction_error_col], UPPER, MIDDLE, LOWER), 
        axis=1
    )
    df["prediction_error_label"] = df.prediction_error_category.map(PREDICTION_ERROR_LABELS) 
    
    return df


def set_error_cutoffs(
    prediction_error: float,
    upper_cutoff: int,
    middle_cutoff: int,
    lower_cutoff: int,
) -> str:
    """
    early (positive values) mean prediction is earlier than actual arrival
    bus comes after prediction (which means you will catch bus)
    highest end, > 5 minutes
    second highest: 3.01 minutes to 5.0 minutes
    second lowest: -3.01 minutes (late) to -5.0 minutes (late)
    lowest end < -5 minutes (late)
    """
    if prediction_error > upper_cutoff:
        return "very_early"
    elif (prediction_error <= upper_cutoff) and (prediction_error > middle_cutoff):
        return "early"
    elif (prediction_error <= middle_cutoff) and (prediction_error > lower_cutoff):
        return "little_early"
    elif (prediction_error <= lower_cutoff) and (prediction_error >= -1 * lower_cutoff):
        return "ontime"
    elif (prediction_error < -1 * lower_cutoff) and (prediction_error >= -1 * middle_cutoff):
        return "little_late"
    elif (prediction_error < -1 * middle_cutoff) and (prediction_error >= -1 * upper_cutoff):
        return "late"
    elif (prediction_error < -1 * upper_cutoff):
        return "very_late"
    else:
        return "unknown"
    

