"""
One-off functions, run once, save datasets for shared use.
"""
import geopandas as gpd
import pandas as pd
import shapely
from calitp_data_analysis import geography_utils, utils
from calitp_data_analysis.sql import to_snakecase
from shared_utils.arcgis_query import query_arcgis_feature_server

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/shared_data/"


def make_county_centroids():
    """
    Find a county's centroids from county polygons.
    """
    URL = "https://opendata.arcgis.com/datasets/" "8713ced9b78a4abb97dc130a691a8695_0.geojson"

    gdf = gpd.read_file(URL).to_crs(geography_utils.CA_StatePlane)
    gdf.columns = gdf.columns.str.lower()

    gdf = (
        gdf.assign(area=gdf.geometry.area)
        # Sort in descending order
        # Ex: LA County, where there are separate polygons for Channel Islands
        # centroid should be based on where the biggest polygon is
        .sort_values(["county_name", "area"], ascending=[True, False])
        .drop_duplicates(subset="county_name")
        .reset_index()
        .to_crs(geography_utils.WGS84)
    )

    # Grab the centroid
    gdf = gdf.assign(
        longitude=(gdf.geometry.centroid.x).round(2),
        latitude=(gdf.geometry.centroid.y).round(2),
    )

    # Make the centroid into a list with [latitude, longitude] format
    gdf = gdf.assign(
        centroid=gdf.apply(lambda x: list([x.latitude, x.longitude]), axis=1),
        zoom=11,
    )[["county_name", "centroid", "zoom"]]

    # Create statewide zoom parameters
    ca_row = {"county_name": "CA", "centroid": [35.8, -119.4], "zoom": 6}

    # Do transpose to get it to display match how columns are displayed
    ca_row2 = pd.DataFrame.from_dict(ca_row, orient="index").T
    gdf2 = gdf.append(ca_row2).reset_index(drop=True)

    # Save as parquet, because lat/lon held in list, not point geometry anymore
    gdf2.to_parquet(f"{GCS_FILE_PATH}ca_county_centroids.parquet")

    print("County centroids exported to GCS")

    return


def make_clean_state_highway_network():
    """
    Create State Highway Network dataset.
    """
    URL = "https://opendata.arcgis.com/datasets/" "77f2d7ba94e040a78bfbe36feb6279da_0.geojson"

    gdf = gpd.read_file(URL)

    keep_cols = ["Route", "County", "District", "RouteType", "Direction", "geometry"]

    gdf = gdf[keep_cols]
    print(f"# rows before dissolve: {len(gdf)}")

    # See if we can dissolve further - use all cols except geometry
    # Should we dissolve further and use even longer lines?
    dissolve_cols = [c for c in list(gdf.columns) if c != "geometry"]

    gdf2 = gdf.dissolve(by=dissolve_cols).reset_index()
    print(f"# rows after dissolve: {len(gdf2)}")

    # Export to GCS
    utils.geoparquet_gcs_export(gdf2, GCS_FILE_PATH, "state_highway_network")


def export_shn_postmiles():
    """
    Create State Highway Network postmiles dataset.
    These are points....maybe we can somehow create line segments?
    """
    URL = "https://caltrans-gis.dot.ca.gov/arcgis/rest/services/" "CHhighway/SHN_Postmiles_Tenth/" "FeatureServer/0/"

    gdf = query_arcgis_feature_server(URL)

    gdf2 = to_snakecase(gdf).drop(columns="objectid")

    utils.geoparquet_gcs_export(gdf2, GCS_FILE_PATH, "state_highway_network_postmiles")

    return


def draw_line_between_points(gdf: gpd.GeoDataFrame, group_cols: list) -> gpd.GeoDataFrame:
    """
    Use the current postmile as the
    starting geometry / segment beginning
    and the subsequent postmile (based on odometer)
    as the ending geometry / segment end.

    Segment goes from current to next postmile.
    """
    # Grab the subsequent point geometry
    # We can drop whenever the last point is missing within
    # a group. If we have 3 points, we can draw 2 lines.
    gdf = gdf.assign(end_geometry=(gdf.groupby(group_cols, group_keys=False).geometry.shift(-1))).dropna(
        subset="end_geometry"
    )

    # Construct linestring with 2 point coordinates
    gdf = (
        gdf.assign(
            line_geometry=gdf.apply(lambda x: shapely.LineString([x.geometry, x.end_geometry]), axis=1).set_crs(
                geography_utils.WGS84
            )
        )
        .drop(columns=["geometry", "end_geometry"])
        .rename(columns={"line_geometry": "geometry"})
    )

    return gdf


def create_postmile_segments(group_cols: list) -> gpd.GeoDataFrame:
    """
    Take the SHN postmiles gdf, group by highway / odometer
    and convert the points into lines.
    We'll lose the last postmile for each highway-direction.
    Segment goes from current postmile point to subseq postmile point.
    """
    gdf = gpd.read_parquet(
        f"{GCS_FILE_PATH}state_highway_network_postmiles.parquet",
        columns=["route", "direction", "odometer", "geometry"],
    )

    # If there are duplicates with highway-direction and odometer
    # (where pm or other column differs slightly),
    # we'll drop and cut as long of a segment we can
    # There may be differences in postmile (relative to county start)
    # and odometer (relative to line's origin).
    gdf2 = (
        gdf.sort_values(group_cols + ["odometer"])
        .drop_duplicates(subset=group_cols + ["odometer"])
        .reset_index(drop=True)
    )

    gdf3 = draw_line_between_points(gdf2, group_cols)

    utils.geoparquet_gcs_export(gdf3, GCS_FILE_PATH, "state_highway_network_postmile_segments")

    return


if __name__ == "__main__":
    # Run functions to create these datasets...store in GCS

    make_county_centroids()

    make_clean_state_highway_network()
    export_shn_postmiles()

    create_postmile_segments(["route", "direction"])
