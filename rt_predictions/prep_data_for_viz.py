"""
Merge downloaded tables, light data cleaning,
and get it ready for visualization.
"""

import gcsfs
import geopandas as gpd
import google.auth
import pandas as pd
import report_utils
from calitp_data_analysis import utils
from outlier_detection import remove_outliers
from rt_msa_utils import PREDICTIONS_GCS, RT_MSA_DICT

credentials, project = google.auth.default()


def categorize_prediction_error(
    df: pd.DataFrame,
    prediction_error_col: str,
    upper_cutoff: int = 5,  # 5 minutes
    middle_cutoff: int = 3,
    lower_cutoff: int = 1,
) -> pd.DataFrame:
    """
    Using exponential equation to determine accuracy means it is
    fairly assertive.
    However, we can convert that to minutes and get a better understanding
    of how far out operators are doing.

    early (positive values) mean prediction is earlier than actual arrival
    bus comes after prediction (which means you will catch bus)
    highest end, > 5 minutes
    second highest: 3.01 minutes to 5.0 minutes
    second lowest: -3.01 minutes (late) to -5.0 minutes (late)
    lowest end < -5 minutes (late)
    """

    def set_error_cutoffs(prediction_error: float, upper_cutoff: int, middle_cutoff: int, lower_cutoff: int) -> str:
        if prediction_error > upper_cutoff:
            return f"{upper_cutoff}+ min early"
        elif (prediction_error <= upper_cutoff) and (prediction_error > middle_cutoff):
            return f"{middle_cutoff}-{upper_cutoff} min early"
        elif (prediction_error <= middle_cutoff) and (prediction_error > lower_cutoff):
            return f"{lower_cutoff}-{middle_cutoff} min early"
        elif (prediction_error <= lower_cutoff) and (prediction_error >= -1 * lower_cutoff):
            return f"{lower_cutoff} min early to {lower_cutoff} min late"
        elif (prediction_error < -1 * lower_cutoff) and (prediction_error >= -1 * middle_cutoff):
            return f"{lower_cutoff}-{middle_cutoff} min late"
        elif (prediction_error < -1 * middle_cutoff) and (prediction_error >= -1 * upper_cutoff):
            return f"{middle_cutoff}-{upper_cutoff} min late"
        elif prediction_error < -1 * upper_cutoff:
            return f"{upper_cutoff}+ min late"
        else:
            return "unknown"

    df["prediction_error_label"] = df.apply(
        lambda x: set_error_cutoffs(x[prediction_error_col], upper_cutoff, middle_cutoff, lower_cutoff), axis=1
    )

    return df


def import_stop_order_by_route(**kwargs) -> pd.DataFrame:
    """ """
    df = pd.read_parquet(
        f"{PREDICTIONS_GCS}{RT_MSA_DICT.dbt_model_downloads.stop_order}.parquet",
        filesystem=gcsfs.GCSFileSystem(),
        **kwargs,
    )

    route_cols = ["gtfs_dataset_name", "route_name", "direction_id"]
    stop_cols = route_cols + ["stop_id"]
    other_cols = [c for c in df.columns if c not in stop_cols and c != "avg_stop_seq"]

    # in case other columns should be kept to see versioned differences,
    # keep those in a list
    df2 = (
        df.sort_values(stop_cols + other_cols)
        .groupby(stop_cols, dropna=False)
        .agg({"avg_stop_seq": "mean", **{c: lambda x: list(set(x)) for c in other_cols}})
        .reset_index()
    )

    df2 = df2.assign(avg_stop_seq=df2.avg_stop_seq.round(2), stop_rank=df2.groupby(route_cols).avg_stop_seq.rank())

    return df2


def merge_stops_with_route_info(filename: str) -> gpd.GeoDataFrame:
    """ """
    stop_gdf = gpd.read_parquet(
        f"{PREDICTIONS_GCS}{filename}.parquet",
        storage_options={"token": credentials.token},
    )
    stop_gdf = (
        stop_gdf.assign(
            bus_catch_likelihood=stop_gdf.pct_tu_predictions_early + stop_gdf.pct_tu_predictions_ontime,
            avg_prediction_spread_minutes=stop_gdf.avg_prediction_spread_minutes.round(2),
        )
        .pipe(report_utils.convert_seconds_to_minutes, "avg_prediction_error_sec")
        .pipe(remove_outliers)
        .pipe(
            categorize_prediction_error,
            "avg_prediction_error_minutes",
            upper_cutoff=5,
            middle_cutoff=3,
            lower_cutoff=1,
        )
    )

    stop_order = import_stop_order_by_route(
        columns=["gtfs_dataset_name", "feed_key", "route_name", "direction_id", "stop_id", "avg_stop_seq"]
    )

    stop_with_route_gdf = pd.merge(
        stop_gdf,
        stop_order.rename(columns={"gtfs_dataset_name": "schedule_name"}),
        on=["schedule_name", "stop_id"],
        how="left",
    ).pipe(report_utils.add_route_direction_column)

    return stop_with_route_gdf


def clean_route_file(filename: str):
    """ """
    route_df = pd.read_parquet(
        f"{PREDICTIONS_GCS}{filename}.parquet",
        filesystem=gcsfs.GCSFileSystem(),
    )

    route_df = (
        route_df.assign(
            bus_catch_likelihood=(route_df.n_predictions_early + route_df.n_predictions_ontime)
            .divide(route_df.n_predictions)
            .round(3),
        )
        .pipe(report_utils.convert_seconds_to_minutes, "avg_prediction_error_sec")
        .pipe(
            categorize_prediction_error,
            "avg_prediction_error_minutes",
            upper_cutoff=5,
            middle_cutoff=3,
            lower_cutoff=1,
        )
        .pipe(report_utils.add_route_direction_column)
    )

    # Add prediction padding, 25th percentile, 75th percentile, and calculate IQR
    group_cols = [
        "month_first_day",
        "day_type",
        "base64_url",
        "tu_name",
        "schedule_base64_url",
        "schedule_name",
        "route_dir_name",
    ]

    # percentiles are scaled or unscaled
    # For scaled, explode and get percentiles to keep, pivot
    PTILES_TO_DISPLAY = [5, 25, 75]
    scaled_df = report_utils.explode_percentiles(
        route_df,
        group_cols,
        array_col="scaled_prediction_error_sec_array",
        ptile_array_col="scaled_prediction_error_sec_percentile_array",
        ptiles_to_keep=PTILES_TO_DISPLAY,
    ).rename(
        columns={
            **{f"p{i}": f"scaled_p{i}" for i in PTILES_TO_DISPLAY},
            "iqr": "scaled_iqr",
            "p5": "scaled_prediction_padding",
        }
    )

    # For unscaled, explode and get the percentiles to keep, pivot, then convert to minutes
    unscaled_df = (
        report_utils.explode_percentiles(
            route_df,
            group_cols,
            array_col="prediction_error_sec_array",
            ptile_array_col="prediction_error_sec_percentile_array",
            ptiles_to_keep=PTILES_TO_DISPLAY,
        )
        .rename(columns={**{f"p{i}": f"p{i}" for i in PTILES_TO_DISPLAY}, "p5": "prediction_padding"})
        .pipe(report_utils.convert_seconds_to_minutes, "p25")
        .pipe(report_utils.convert_seconds_to_minutes, "p75")
        .pipe(report_utils.convert_seconds_to_minutes, "prediction_padding")
        .pipe(report_utils.convert_seconds_to_minutes, "iqr")
    )

    # prediction padding is absolute value of 5th percentile, make that explicit here
    percentiles_df = pd.merge(
        scaled_df.assign(scaled_prediction_padding=scaled_df.scaled_prediction_padding.abs()),
        unscaled_df.assign(prediction_padding=unscaled_df.prediction_padding.abs()),
        on=group_cols,
        how="inner",
    )

    route_df2 = pd.merge(route_df, percentiles_df, on=group_cols, how="inner")

    return route_df2


if __name__ == "__main__":

    DOWNLOADED_DICT = RT_MSA_DICT.dbt_model_downloads
    PROCESSED_DICT = RT_MSA_DICT.rt_schedule_models

    TU_STOP_DOWNLOADED = DOWNLOADED_DICT.weekday_stop_grain
    TU_STOP_CLEANED = PROCESSED_DICT.weekday_stop_with_route

    stop_with_route_gdf = merge_stops_with_route_info(TU_STOP_DOWNLOADED)

    utils.geoparquet_gcs_export(stop_with_route_gdf, PREDICTIONS_GCS, TU_STOP_CLEANED)

    TU_ROUTE_DOWNLOADED = DOWNLOADED_DICT.weekday_route_direction_grain
    TU_ROUTE_CLEANED = PROCESSED_DICT.weekday_route_direction

    route_cleaned = clean_route_file(TU_ROUTE_DOWNLOADED)
    route_cleaned.to_parquet(f"{PREDICTIONS_GCS}{TU_ROUTE_CLEANED}.parquet", filesystem=gcsfs.GCSFileSystem())
