"""
Data prep related to adding new columns, rounding values,
renaming columns.

RT stop metrics + intermediate table stop_order_by_route
stops ordered by route-direction.
Stops are more interpretable when filtered against
routes.
"""

import gcsfs
import geopandas as gpd
import google.auth
import pandas as pd
from rt_msa_utils import PREDICTIONS_GCS, RT_MSA_DICT

credentials, project = google.auth.default()

DAYTYPE_ORDER_DICT = {"Weekday": 1, "Saturday": 2, "Sunday": 3}


def explode_percentiles(
    df: pd.DataFrame, group_cols: list, array_col: str, ptile_array_col: str, ptiles_to_keep: list, pivoted: bool = True
):
    subset_df = (
        df[group_cols + [array_col, ptile_array_col]]
        .explode([array_col, ptile_array_col])
        .reset_index(drop=True)
        .query(f"{ptile_array_col} in @ptiles_to_keep")
        .astype({array_col: float, ptile_array_col: int})
    )

    if pivoted:
        pivoted_df = subset_df.pivot(index=group_cols, columns=ptile_array_col, values=array_col).reset_index()

        pivoted_df = pivoted_df.rename(columns={c: f"p{c}" for c in ptiles_to_keep})

        # if IQR can be calculated, calculate it as a new column
        if (25 in ptiles_to_keep) and (75 in ptiles_to_keep):
            pivoted_df = pivoted_df.assign(iqr=pivoted_df.p75 - pivoted_df.p25)

        return pivoted_df
    else:
        return subset_df


def convert_seconds_to_minutes(
    df: pd.DataFrame,
    seconds_col: str,
) -> pd.DataFrame:
    """
    This one is done repeatedly, but sometimes the array
    needs to be exploded first.
    """
    minute_col = seconds_col.replace("_sec", "_minutes")
    df[minute_col] = df[seconds_col].divide(60).round(2)

    return df


def add_route_direction_column(df: pd.DataFrame) -> pd.DataFrame:
    """ """
    df = df.assign(route_dir_name=df.route_name + "_" + df.direction_id.astype(str))

    return df


def import_stop_df(**kwargs) -> gpd.GeoDataFrame:
    """ """
    filename = f"{PREDICTIONS_GCS}{RT_MSA_DICT.rt_schedule_models.weekday_stop_with_route}.parquet"

    # If specific columns are defined, and one of those is geometry, use geopandas
    if "columns" in kwargs and "geometry" in kwargs["columns"]:
        df = gpd.read_parquet(filename, storage_options={"token": credentials.token}, **kwargs)
    else:
        df = pd.read_parquet(filename, filesystem=gcsfs.GCSFileSystem(), **kwargs)
    return df


def import_route_df(**kwargs) -> gpd.GeoDataFrame:
    """ """
    filename = f"{PREDICTIONS_GCS}{RT_MSA_DICT.rt_schedule_models.weekday_route_direction}.parquet"

    # If specific columns are defined, and one of those is geometry, use geopandas
    if "columns" in kwargs and "geometry" in kwargs["columns"]:
        df = gpd.read_parquet(filename, storage_options={"token": credentials.token}, **kwargs)
    else:
        df = pd.read_parquet(filename, filesystem=gcsfs.GCSFileSystem(), **kwargs)

    return df


def merge_route_to_stop_for_nanoplot(route_df: pd.DataFrame, stop_df: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    In great tables, display route-direction metrics with
    individual stop's avg_prediction_error_minute (ordered by stop_rank).
    """
    operator_group_cols = [
        "schedule_name",
        "schedule_base64_url",
        # "tu_base64_url", # route doesn't have tu_base64_url...might have already aggregated?
        "month_first_day",
        "day_type",
    ]

    group_cols = operator_group_cols + ["route_dir_name"]

    stop_metrics_by_route = (
        stop_df.sort_values(group_cols + ["stop_rank"])
        .groupby(group_cols, dropna=False)
        .agg(
            {
                "stop_id": lambda x: list(x),
                "stop_name": lambda x: list(x),
                "avg_prediction_error_minutes": lambda x: list(round(x, 2)),
                "avg_prediction_spread_minutes": lambda x: list(round(x, 2)),
            }
        )
        .reset_index()
        .rename(
            columns={
                "avg_prediction_error_minutes": "prediction_error_by_stop",
                "avg_prediction_spread_minutes": "prediction_spread_by_stop",
            }
        )
    )

    # Highlight which stops have late predictions, on avg, and these cause riders to miss bus
    early_stops = (
        stop_df[stop_df.prediction_error_label == "3-5 min early"]
        .sort_values(group_cols + ["stop_rank"])
        .groupby(group_cols, dropna=False)
        .agg({"stop_id": lambda x: len(list(x))})
        .reset_index()
        .rename(
            columns={
                "stop_id": "n_early_stops",
            }
        )
    )
    late_stops = (
        stop_df[stop_df.prediction_error_label == "3-5 min late"]
        .sort_values(group_cols + ["stop_rank"])
        .groupby(group_cols, dropna=False)
        .agg({"stop_id": lambda x: len(list(x))})
        .reset_index()
        .rename(
            columns={
                "stop_id": "n_late_stops",
            }
        )
    )

    early_late_cols = ["n_early_stops", "n_late_stops"]

    df = (
        pd.merge(
            route_df,
            stop_metrics_by_route,
            on=group_cols,
            how="inner",
        )
        .merge(early_stops, on=group_cols, how="left")
        .merge(late_stops, on=group_cols, how="left")
        .fillna({c: 0 for c in early_late_cols})
        .astype({c: int for c in early_late_cols})
    )

    return df


def import_operator_df(**kwargs) -> pd.DataFrame:
    """ """
    # subset columns
    df = pd.read_parquet(
        f"{PREDICTIONS_GCS}{RT_MSA_DICT.dbt_model_downloads.weekday_operator_grain}.parquet",
        filesystem=gcsfs.GCSFileSystem(),
        **kwargs,
    )

    # in deploy script, need to drop rows where any of this is present
    # if we don't, we'll have dupes
    # can't drop duplicates earlier with arrays
    # operators may not have both RT data, so sometimes one will be missing
    for c in ["schedule_name", "vp_name", "tu_name"]:
        if c in df.columns:
            df = df.dropna(subset=c).reset_index(drop=True)

    return df
