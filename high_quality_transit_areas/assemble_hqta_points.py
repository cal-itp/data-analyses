"""
Combine all the points for HQ transit open data portal.

Request: Thank you for this data. It would be useful for us to get the
HQTA stops as a point data file, not a polygon. Also, if you could
differentiate between train, bus, BRT, and ferry stop that would be
immensely helpful. Let me know if it is possible to get the data in this format.

Now included MPO-provided planned major transit stops.
"""

import datetime
import sys

import _utils
import geopandas as gpd
import intake
import numpy as np
import pandas as pd
from calitp_data_analysis import geography_utils, get_fs, utils
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from calitp_data_analysis.sql import query_sql
from loguru import logger
from shared_utils import gtfs_utils_v2
from update_vars import (
    EXPORT_PATH,
    GCS_FILE_PATH,
    MPO_DATA_PATH,
    PROJECT_CRS,
    analysis_date,
)

gcsgp = GCSGeoPandas()
fs = get_fs()
catalog = intake.open_catalog("*.yml")


def combine_stops_by_hq_types(crs: str) -> gpd.GeoDataFrame:
    """
    Concatenate combined hqta points across all categories then merge in
    the maximum arrivals for each stop (keep if it shows up in hqta_points)
    with left merge.
    """
    rail_ferry_brt = catalog.rail_brt_ferry_stops().read().to_crs(crs)
    major_stop_bus = catalog.major_stop_bus().read().to_crs(crs)
    major_stop_bus_branching = catalog.major_stop_bus_branching().read().to_crs(crs)
    stops_in_corridor = catalog.stops_in_hq_corr().read().to_crs(crs)

    trip_count_cols = ["am_max_trips_hr", "pm_max_trips_hr"]

    max_arrivals = pd.read_parquet(
        f"{GCS_FILE_PATH}max_arrivals_by_stop.parquet",
        columns=["schedule_gtfs_dataset_key", "stop_id"] + trip_count_cols,
    ).pipe(_utils.primary_rename)

    # Combine AM max and PM max into 1 column
    # if am_max_trips = 4 and pm_max_trips = 5, we'll choose 4.
    max_arrivals = max_arrivals.assign(avg_trips_per_peak_hr=max_arrivals[trip_count_cols].min(axis=1)).drop(
        columns=trip_count_cols
    )

    hqta_points_combined = pd.concat(
        [
            major_stop_bus,
            major_stop_bus_branching,
            stops_in_corridor,
            rail_ferry_brt,
        ],
        axis=0,
    )

    # Merge in max arrivals
    with_stops = (
        pd.merge(hqta_points_combined, max_arrivals, on=["schedule_gtfs_dataset_key_primary", "stop_id"], how="left")
        .fillna({"avg_trips_per_peak_hr": 0})
        .astype({"avg_trips_per_peak_hr": "int"})
    )

    keep_stop_cols = [
        "schedule_gtfs_dataset_key_primary",
        "schedule_gtfs_dataset_key_secondary",
        "stop_id",
        "geometry",
        "hqta_type",
        "avg_trips_per_peak_hr",
        "hqta_details",
    ]

    with_stops = with_stops.assign(hqta_details=with_stops.apply(_utils.add_hqta_details, axis=1))[keep_stop_cols]

    return with_stops


def get_agency_crosswalk() -> pd.DataFrame:
    """
    Simplified version using analysis_name from warehouse
    """

    query = """
    SELECT
    key AS schedule_gtfs_dataset_key,
    analysis_name AS agency,
    base64_url
    FROM
    cal-itp-data-infra.mart_transit_database.dim_gtfs_datasets
    WHERE _is_current = TRUE
    AND analysis_name IS NOT NULL
    """

    df = query_sql(query)
    return df


def add_route_agency_info(gdf: gpd.GeoDataFrame, analysis_date: str) -> gpd.GeoDataFrame:
    """
    Make sure route info is filled in for all stops.

    Add agency names by merging it in with our crosswalk
    and populate primary and secondary operator information.
    """
    stop_with_route_crosswalk = catalog.stops_info_crosswalk().read()

    #  TODO lookback and concat
    # agency_info = get_agency_crosswalk(analysis_date)
    agency_info = get_agency_crosswalk()

    # Make sure all the stops have route_id
    gdf2 = pd.merge(
        gdf,
        stop_with_route_crosswalk[["schedule_gtfs_dataset_key", "stop_id", "route_id"]]
        .drop_duplicates()
        .pipe(_utils.primary_rename),
        on=["schedule_gtfs_dataset_key_primary", "stop_id"],
        how="inner",
    )

    # Make sure gtfs_dataset_name and organization columns are added
    gdf3 = pd.merge(
        gdf2, agency_info.add_suffix("_primary"), on="schedule_gtfs_dataset_key_primary", how="inner"
    ).merge(
        agency_info.add_suffix("_secondary"),
        on="schedule_gtfs_dataset_key_secondary",
        how="left",
        # left bc we don't want to drop rows that have secondary operator
    )

    return gdf3


def final_processing_gtfs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Final steps for getting dataset ready for Geoportal.
    Subset to columns, drop duplicates, sort for readability,
    always project into WGS84.
    """
    # Clip to CA -- remove ferry or else we're losing it in the clip
    not_ferry = gdf[gdf.hqta_type != "major_stop_ferry"].pipe(_utils.clip_to_ca)

    is_ferry = gdf[gdf.hqta_type == "major_stop_ferry"]

    gdf2 = pd.concat([not_ferry, is_ferry], axis=0).reset_index(drop=True)

    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()

    keep_cols = [
        "agency_primary",
        "hqta_type",
        "stop_id",
        "route_id",
        "hqta_details",
        "agency_secondary",
        # include these as stable IDs?
        "base64_url_primary",
        "base64_url_secondary",
        "avg_trips_per_peak_hr",
        "mpo",
        "geometry",
    ]
    gdf3 = (
        gdf2[(gdf2.schedule_gtfs_dataset_key_primary.isin(public_feeds))]
        .reindex(columns=keep_cols)
        .drop_duplicates(subset=["agency_primary", "hqta_type", "stop_id", "route_id"])
        .sort_values(["agency_primary", "hqta_type", "stop_id"])
        .reset_index(drop=True)
        .to_crs(geography_utils.WGS84)
    )

    return gdf3


def read_standardize_mpo_input(mpo_data_path=MPO_DATA_PATH, gcsgp=gcsgp, fs=fs) -> gpd.GeoDataFrame:
    """
    Read in mpo-provided planned major transit stops and enforce schema.
    """
    mpo_names = [x.split("/")[-1].split(".")[0] for x in fs.ls(MPO_DATA_PATH) if x.split("/")[-1] != "mpo_input"]

    mpo_gdfs = []
    for mpo_name in mpo_names:
        mpo_gdf = gcsgp.read_file(f"{MPO_DATA_PATH}{mpo_name}.geojson")
        required_cols = ["mpo", "hqta_type", "plan_name"]
        optional_cols = ["stop_id", "avg_trips_per_peak_hr", "agency_primary"]
        all_cols = required_cols + optional_cols + ["geometry"]
        assert set(required_cols).issubset(mpo_gdf.columns)
        filter_cols = [col for col in all_cols if col in mpo_gdf.columns]
        mpo_gdf = mpo_gdf[filter_cols]
        mpo_gdfs += [mpo_gdf]
    return pd.concat(mpo_gdfs)


if __name__ == "__main__":

    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    start = datetime.datetime.now()
    mpos = [x.split("/")[-1].split(".")[0] for x in fs.ls(MPO_DATA_PATH) if x.split("/")[-1]]

    # Combine all the points data and merge in max_arrivals
    hqta_points_combined = combine_stops_by_hq_types(crs=PROJECT_CRS)

    # Add in route_id and agency info
    hqta_points_with_info = add_route_agency_info(hqta_points_combined, analysis_date)

    gdf = final_processing_gtfs(hqta_points_with_info)

    # Add MPO-provided planned major transit stops
    planned_stops = read_standardize_mpo_input().to_crs(geography_utils.WGS84)
    planned_stops = planned_stops.assign(hqta_details=planned_stops.apply(_utils.add_hqta_details, axis=1))
    gdf = pd.concat([gdf, planned_stops]).astype({"stop_id": str, "avg_trips_per_peak_hr": np.float64})

    # Export to GCS
    # Stash this date's into its own folder
    utils.geoparquet_gcs_export(
        gdf,
        EXPORT_PATH,
        "ca_hq_transit_stops",
    )

    # Overwrite most recent version (other catalog entry constantly changes)
    utils.geoparquet_gcs_export(
        gdf,
        GCS_FILE_PATH,
        "hqta_points",
    )

    end = datetime.datetime.now()
    logger.info(f"D1_assemble_hqta_points {analysis_date} " f"execution time: {end - start}")
