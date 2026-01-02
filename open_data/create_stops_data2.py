"""
Create `ca_transit_stops` to publish to Geoportal.
"""

import geopandas as gpd
import google.auth
import pandas as pd
from calitp_data_analysis import utils
from update_vars import OPEN_DATA_GCS, analysis_month

credentials, _ = google.auth.default()


def prep_stops(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Aggregate day_type (weekday/Sat/Sun) to all stops that month.
    """
    stop_group_cols = ["name", "stop_id", "stop_name"]
    # Group across day_types
    stop_geom = gdf[stop_group_cols + ["geometry"]].drop_duplicates()

    gdf2 = (
        gdf.groupby(stop_group_cols + ["month_first_day"])
        .agg({"total_stop_arrivals": "sum", "n_transit_modes": "max", "n_days": "sum"})
        .reset_index()
    )

    gdf3 = pd.merge(stop_geom, gdf2, on=stop_group_cols, how="inner")

    gdf3 = gdf3.assign(
        # round daily arrivals to nearest integer
        # calculate this again because we aggregated and weekdays are 5x and weekends are 2x
        daily_stop_arrivals=gdf3.total_stop_arrivals.divide(gdf3.n_days)
        .round(0)
        .astype(int)
    )

    return gdf3


def rename_stop_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Put all the renaming here.
    """
    keep_cols = [
        # from stops
        "name",
        "stop_id",
        "stop_name",
        "n_arrivals",
        "geometry",
        # from stop times
        "routetypes",
        # still need
        # "n_routes", "route_ids_served",
        # "n_hours_in_service",
        # need to calculate
        # "meters_to_ca_state_highway",
        # from bridge / crosswalk
        "analysis_name",
        "district_name",
        # "schedule_base64_url" # this might not be consistent
        # schedule_source_record_id would be more consistent
    ]

    gdf = gdf[keep_cols].rename(columns={"daily_arrivals": "n_arrivals", "n_transit_modes": "routetypes"})

    return gdf


def prep_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Put whatever needs to be done to crosswalk here.
    Maybe just combine caltrans_district / caltrans_district_name?
    """
    df = df.assign(
        district_name=df.caltrans_district.astype(str).str.zfill(2) + " - " + df.caltrans_district_name
    ).drop(columns=["caltrans_district", "caltrans_district_name"])

    return df


def publish_stops(analysis_month: str):
    stops = gpd.read_parquet(
        f"{OPEN_DATA_GCS}stops_{analysis_month}.parquet",
        storage_options={"token": credentials.token},
    ).pipe(prep_stops)

    crosswalk = pd.read_parquet(f"{OPEN_DATA_GCS}bridge_gtfs_analysis_name_x_ntd.parquet").pipe(prep_crosswalk)

    stops2 = pd.merge(stops, crosswalk.rename(columns={"schedule_gtfs_dataset_name": "name"}), on=["name"], how="inner")

    # TODO1 (warehouse): missing some columns that are needed from stop_times
    # route_ids_served, n_hours_in_service
    # this needs to be part of new stop_times + trips + stops table in warehouse

    # TODO2 (add SHN derived columns): use existing function and work it into this
    # TODO3 (standardize columns for Geoportal): pipe through rename_stop_columns

    return stops2


if __name__ == "__main__":

    stops = publish_stops(analysis_month)

    utils.geoparquet_gcs_export(stops, OPEN_DATA_GCS, f"export/ca_transit_stops_{analysis_month}")
