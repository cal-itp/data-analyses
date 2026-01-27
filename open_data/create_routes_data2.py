"""
Create `ca_transit_routes` to publish to Geoportal.
"""

import datetime
import sys

import gcsfs
import geopandas as gpd
import google.auth
import pandas as pd
from calitp_data_analysis import geography_utils, utils
from loguru import logger
from update_vars import OPEN_DATA_GCS, analysis_month

credentials, _ = google.auth.default()

MONTHLY_ROUTES_COLS = [
    # route-shapes for month, use these to aggregate metrics
    "name",
    "shape_id",
    "route_name",  # parse this out into route_id, route_name
    "route_type",
    # route-shape metrics
    "n_trips",
    "geometry",
]


def prep_route_shapes(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Aggregate day_type (weekday/Sat/Sun) to all shape-route
    combinations that month.
    """
    route_group_cols = ["name", "shape_id", "route_name"]
    # Group across day_types
    shape_geom = gdf[route_group_cols + ["geometry"]].drop_duplicates()

    # There are a couple rows that do get aggregated here
    gdf2 = (
        gdf.sort_values(route_group_cols + ["n_trips"], ascending=[True for c in route_group_cols] + [False])
        .groupby(route_group_cols)
        .agg({"n_trips": "sum", "route_type": "first"})
        .reset_index()
    )

    gdf3 = pd.merge(shape_geom, gdf2, on=route_group_cols, how="inner")

    # Parse out route_name here, which gets combined and cleaned and standardized
    # https://dbt-docs.dds.dot.ca.gov/index.html#!/model/model.calitp_warehouse.fct_monthly_scheduled_trips
    gdf3 = gdf3.assign(
        route_length_feet=gdf3.geometry.to_crs(geography_utils.CA_NAD83Albers_ft).length.round(2),
        route_id=gdf3.route_name.str.split("__", expand=True)[0],
        route_name=gdf3.route_name.str.split("__", expand=True)[1],
    )

    return gdf3


def rename_route_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Put all the renaming here.
    """
    gdf = gdf.rename(
        columns={
            "caltrans_district_full": "district_name",
        }
    )

    return gdf


def add_shn_derived_columns(shapes: gpd.GeoDataFrame, buffer_amt: int = 50) -> gpd.GeoDataFrame:
    """
    Buffer and dissolve State Highway Network linestrings to SHN Route.
    Use geopandas overlay to calculate % of transit route (shape) that
    falls within 50 ft of SHN.

    Add a list of highways that the the transit route (shape) intersects
    with (50 ft buffer around SHN).
    """
    SHARED_GCS = "gs://calitp-analytics-data/data-analyses/shared_data/"

    shn = gpd.read_parquet(f"{SHARED_GCS}state_highway_network.parquet", storage_options={"token": credentials.token})

    # Buffer and simply the geometry slightly
    # when we have geopandas >= 0.1, we can use simplify_coverage to help simplify polygons
    shn_buff = (
        shn[["Route", "geometry"]]
        .assign(geometry=shn.geometry.to_crs("EPSG:2229").buffer(buffer_amt))
        .dissolve(by="Route")
        .reset_index()
    )

    shn_buff = shn_buff.assign(geometry=shn_buff.geometry.simplify(tolerance=25))

    # Overlay transit routes with the SHN geographies.
    intersect_gdf = gpd.overlay(shapes.to_crs(shn_buff.crs), shn_buff, how="intersection", keep_geom_type=True)

    # Calcuate the percent of the transit route that runs on a highway, round it up and
    # multiply it by 100. These % have 3 decimal places.
    intersect_gdf = intersect_gdf.assign(
        pct_route_on_hwy=(intersect_gdf.geometry.length / intersect_gdf.route_length_feet).round(5) * 100,
    )

    route_group_cols = ["name", "shape_id", "route_name"]

    # for each shape_id-route_name, get a list of unique SHN routes that it intersected with
    # and write as a string. Instead of [5, 10], write as 5, 10.
    # coerce as string so that we can fillna later with only string values.
    intersect_gdf2 = (
        intersect_gdf.groupby(route_group_cols)
        .agg({"pct_route_on_hwy": "sum", "Route": lambda x: ", ".join(map(str, sorted(set(x))))})
        .reset_index()
        # distinguish between transit routes and SHN Route column
        .rename(columns={"Route": "shn_route"})
    )

    shapes_with_shn = pd.merge(shapes, intersect_gdf2, on=route_group_cols, how="left").fillna({"pct_route_on_hwy": 0})

    # routes can intersect many highways, causing the length to get counted multiple times
    # so let's set sum back to 100 max.
    # For Amtrak, which primarily runs outside CA, if we round to 3 decimal places, we get like 0.03% of its length is within 50 ft of SHN.
    shapes_with_shn = shapes_with_shn.assign(
        pct_route_on_hwy=shapes_with_shn.apply(
            lambda x: round(x.pct_route_on_hwy, 1) if x.pct_route_on_hwy <= 100 else 100, axis=1
        ),
        shn_route=shapes_with_shn.shn_route.fillna(f"not_{buffer_amt}ft_from_shn"),
    )

    return shapes_with_shn.to_crs(shapes.crs)


def publish_routes(analysis_month: str) -> gpd.GeoDataFrame:
    """
    Import downloaded mart_gtfs_rollup.fct_monthly_routes,
    add some columns easily derived,
    add state highway network derived columns,
    rename for publishing,
    and merge in bridge table to exclude feeds we don't want to publish.
    """
    HWY_BUFFER_FT = 50

    routes = (
        gpd.read_parquet(
            f"{OPEN_DATA_GCS}routes_{analysis_month}.parquet",
            storage_options={"token": credentials.token},
            columns=MONTHLY_ROUTES_COLS,
        )
        .pipe(prep_route_shapes)
        .pipe(add_shn_derived_columns, buffer_amt=HWY_BUFFER_FT)
    )

    # need to drop dupes because of fanout from other columns we don't need in this context,
    # like organization_source_record_id or ntd_id
    crosswalk = pd.read_parquet(
        f"{OPEN_DATA_GCS}bridge_gtfs_analysis_name_x_ntd.parquet",
        columns=["schedule_gtfs_dataset_name", "analysis_name", "caltrans_district_full"],
        filesystem=gcsfs.GCSFileSystem(),
    ).drop_duplicates()

    # Merge in crosswalk, which will filter out the feeds we don't want to publish
    routes2 = pd.merge(
        routes, crosswalk.rename(columns={"schedule_gtfs_dataset_name": "name"}), on=["name"], how="inner"
    )

    routes3 = rename_route_columns(routes2)

    return routes3


if __name__ == "__main__":

    LOG_FILE = "./logs/open_data.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    start = datetime.datetime.now()

    routes = publish_routes(analysis_month)

    utils.geoparquet_gcs_export(routes, OPEN_DATA_GCS, f"export/ca_transit_routes_{analysis_month}")

    end = datetime.datetime.now()
    logger.info(f"{analysis_month}: export routes: {end - start}")
