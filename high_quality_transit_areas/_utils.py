"""
Shared utility functions for HQTA
"""

import geopandas as gpd
import intake
import pandas as pd
from calitp_data_analysis.sql import query_sql
from shared_utils import portfolio_utils

catalog = intake.open_catalog("catalog.yml")


def add_hqta_details(row) -> str:
    """
    Add HQTA details of why nulls are present
    based on feedback from open data users.
    """
    if "mpo" in row.index and isinstance(row.mpo, str) and row.mpo:
        return "mpo_rtp_planned_major_stop"

    if row.hqta_type == "major_stop_bus":
        if row.schedule_gtfs_dataset_key_primary != row.schedule_gtfs_dataset_key_secondary:
            return "intersection_2_bus_routes_different_operators"
        else:
            return "intersection_2_bus_routes_same_operator"

    elif row.hqta_type == "hq_corridor_bus":
        if row.avg_trips_per_peak_hr >= 4:
            return "corridor_frequent_stop"
        else:
            return "corridor_other_stop"

    elif row.hqta_type in ["major_stop_ferry", "major_stop_brt", "major_stop_rail"]:
        return row.hqta_type + "_single_operator"


def primary_rename(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"schedule_gtfs_dataset_key": "schedule_gtfs_dataset_key_primary"})


def clip_to_ca(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Clip to CA boundaries.
    """
    ca = catalog.ca_boundary().read().to_crs(gdf.crs)

    gdf2 = gdf.clip(ca, keep_geom_type=False).reset_index(drop=True)

    return gdf2


def append_analysis_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicates based on analysis_name, add it to columns
    """
    cols = df.columns
    lookback_analysis_name = []
    for date in df.analysis_date.unique():
        subset = df.query("analysis_date == @date")
        subset = portfolio_utils.standardize_operator_info_for_exports(subset, date=date)
        subset = subset[list(cols) + ["analysis_name"]]
        lookback_analysis_name += [subset]
    lookback_analysis_name = pd.concat(lookback_analysis_name)

    return lookback_analysis_name


def get_agency_crosswalk() -> pd.DataFrame:
    """
    Simplified version using analysis_name from warehouse.
    Consider broader lookback refactor, using rollup tables once count bug fixed.
    """

    query = """
    SELECT
    key AS schedule_gtfs_dataset_key,
    analysis_name AS agency,
    base64_url
    FROM
    cal-itp-data-infra.mart_transit_database.dim_gtfs_datasets
    WHERE _valid_to >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY))
    AND analysis_name IS NOT NULL
    """

    df = query_sql(query)
    return df
