"""
Create `ca_transit_stops` to publish to Geoportal.
"""

import datetime
import sys

import gcsfs
import geopandas as gpd
import google.auth
import intake
import pandas as pd
from calitp_data_analysis import geography_utils, utils
from loguru import logger
from update_vars import OPEN_DATA_GCS, analysis_month

credentials, _ = google.auth.default()
catalog = intake.open_catalog("../_shared_utils/shared_utils/shared_data_catalog.yml")

MONTHLY_STOPS_COLS = [
    # stop by day_type, use these to aggregate metrics
    "name",
    "stop_id",
    "stop_name",
    "n_days",
    "day_type",
    # stop metrics
    "total_stop_arrivals",
    "geometry",
    "route_type_array",  # renamed to routetypes
    "route_id_array",  # renamed to route_ids_served, use this to calculate n_routes
    "n_hours_in_service",
]


def prep_stops(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Aggregate day_type (weekday/Sat/Sun) to all stops that month.
    """
    stop_group_cols = ["name", "stop_id", "stop_name"]
    # Group across day_types
    stop_geom = gdf[stop_group_cols + ["geometry"]].drop_duplicates()

    gdf2 = (
        gdf.sort_values(stop_group_cols + ["total_stop_arrivals"], ascending=[True for c in stop_group_cols] + [False])
        .groupby(stop_group_cols)
        .agg(
            {
                "total_stop_arrivals": "sum",
                "n_days": "sum",
                "route_type_array": "first",
                "route_id_array": "first",
                "n_hours_in_service": "max",  # if these differ, choose the one that's the highest (weekday)
            }
        )
        .reset_index()
    )

    gdf3 = pd.merge(stop_geom, gdf2, on=stop_group_cols, how="inner")

    gdf3 = gdf3.assign(
        # round daily arrivals to nearest integer
        # calculate this again because we aggregated and weekdays are 5x and weekends are 2x
        n_arrivals=gdf3.total_stop_arrivals.divide(gdf3.n_days).round(0).astype(int),
        n_routes=gdf3.apply(lambda x: len(list(x.route_id_array)), axis=1),
    ).drop(columns=["n_days"])

    return gdf3


def rename_stop_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Put all the renaming here.
    """
    gdf = gdf.rename(
        columns={
            "daily_arrivals": "n_arrivals",
            "route_type_array": "routetypes",
            "route_id_array": "route_ids_served",
            "caltrans_district_full": "district_name",
        }
    )

    return gdf


def add_distance_to_state_highway(stops: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Bring in State Highway Network gdf and add a column that tells us
    distance (in meters) between stop and SHN.
    For stops outside of CA, this will not be that meaningful.
    Using a dissolve takes a long time. Instead, opt for gpd.sjoin_nearest,
    which allows us to return a distance column, and if there are multiple
    rows, we'll keep the closest distance.

    See discussion in:
    https://github.com/cal-itp/data-analyses/issues/1182
    https://github.com/cal-itp/data-analyses/issues/1321
    https://github.com/cal-itp/data-analyses/issues/1397
    """
    orig_crs = stops.crs

    shn = catalog.state_highway_network.read()[["District", "geometry"]].to_crs(geography_utils.CA_NAD83Albers_m)

    stop_cols = ["name", "stop_id", "stop_name"]

    nearest_shn_result = (
        gpd.sjoin_nearest(
            stops[stop_cols + ["geometry"]].to_crs(geography_utils.CA_NAD83Albers_m),
            shn,
            distance_col="meters_to_ca_state_highway",
        )
        .sort_values(stop_cols + ["meters_to_ca_state_highway"])
        .drop_duplicates(subset=stop_cols)
        .reset_index(drop=True)
    )

    stops2 = pd.merge(stops, nearest_shn_result[stop_cols + ["meters_to_ca_state_highway"]], on=stop_cols, how="inner")

    stops2 = stops2.assign(meters_to_ca_state_highway=stops2.meters_to_ca_state_highway.round(1))

    return stops2.to_crs(orig_crs)


def publish_stops(analysis_month: str) -> gpd.GeoDataFrame:
    """
    Import downloaded mart_gtfs_rollup.fct_monthly_scheduled_stops,
    aggregate across day_type,
    add state highway network derived columns,
    rename for publishing,
    and merge in bridge table to exclude feeds we don't want to publish.
    """
    stops = (
        gpd.read_parquet(
            f"{OPEN_DATA_GCS}stops_{analysis_month}.parquet",
            storage_options={"token": credentials.token},
            columns=MONTHLY_STOPS_COLS,
        )
        .pipe(prep_stops)
        .pipe(add_distance_to_state_highway)
    )

    crosswalk = pd.read_parquet(
        f"{OPEN_DATA_GCS}bridge_gtfs_analysis_name_x_ntd.parquet",
        columns=["schedule_gtfs_dataset_name", "analysis_name", "caltrans_district_full"],
        filesystem=gcsfs.GCSFileSystem(),
    ).drop_duplicates()  # need this because we might have dupes that differ on other columns in bridge, like organization_source_record_id

    # Merge in crosswalk, which will filter out the feeds we don't want to publish
    stops2 = pd.merge(stops, crosswalk.rename(columns={"schedule_gtfs_dataset_name": "name"}), on=["name"], how="inner")

    # this is unique on ["name", "stop_id", "stop_name"]
    # there are dupes where same stop_id has slightly different stop_names
    stops3 = rename_stop_columns(stops2)

    return stops3


if __name__ == "__main__":

    LOG_FILE = "./logs/open_data.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    start = datetime.datetime.now()

    stops = publish_stops(analysis_month)

    utils.geoparquet_gcs_export(stops, OPEN_DATA_GCS, f"export/ca_transit_stops_{analysis_month}")

    end = datetime.datetime.now()
    logger.info(f"{analysis_month}: export stops: {end - start}")
