"""
Create `ca_transit_routes` to publish to Geoportal.
"""

import geopandas as gpd
import google.auth
import pandas as pd
from calitp_data_analysis import utils
from create_stops_data2 import prep_crosswalk
from update_vars import OPEN_DATA_GCS, analysis_month

credentials, _ = google.auth.default()


def prep_route_shapes(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Aggregate day_type (weekday/Sat/Sun) to all shape-route
    combinations that month.
    """
    route_group_cols = ["name", "shape_id", "route_name"]
    # Group across day_types
    shape_geom = gdf[route_group_cols + ["geometry"]].drop_duplicates()

    gdf2 = (
        gdf.groupby(route_group_cols + ["month_first_day"])
        .agg(
            {
                "n_trips": "sum",
            }
        )
        .reset_index()
    )

    gdf3 = pd.merge(shape_geom, gdf2, on=route_group_cols, how="inner")

    # should route_name be split to show route_id and route_name?
    return gdf3


def rename_route_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Put all the renaming here.
    """
    keep_cols = [
        # from shapes
        "name",
        "route_name",
        "shape_id",
        "route_type",
        "n_trips",
        "geometry",
        # from trips
        "route_ids",
        # calculate
        "on_shn"  # dummy,
        "shn_districts",
        "pct_route_on_hwy_all_districts",
        # from bridge
        "analysis_name",
        # schedule_source_record_id
    ]

    gdf = gdf[keep_cols].rename(columns={})

    return gdf


def publish_routes(analysis_month: str):
    routes = gpd.read_parquet(
        f"{OPEN_DATA_GCS}routes_{analysis_month}.parquet",
        storage_options={"token": credentials.token},
    ).pipe(prep_route_shapes)

    crosswalk = pd.read_parquet(f"{OPEN_DATA_GCS}bridge_gtfs_analysis_name_x_ntd.parquet").pipe(prep_crosswalk)

    routes2 = pd.merge(
        routes, crosswalk.rename(columns={"schedule_gtfs_dataset_name": "name"}), on=["name"], how="inner"
    )

    # TODO1 route_id unparsed from route_name?
    # TODO2 (add SHN derived columns): use existing function and work it into this
    # TODO3 (standardize columns for Geoportal): pipe through rename_routecolumns

    return routes2


if __name__ == "__main__":

    routes = publish_routes(analysis_month)

    utils.geoparquet_gcs_export(routes, OPEN_DATA_GCS, f"export/ca_transit_routes_{analysis_month}")
