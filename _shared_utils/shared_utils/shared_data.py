"""
One-off functions, run once, save datasets for shared use.
"""
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from calitp_data_analysis import geography_utils, utils
from calitp_data_analysis.sql import to_snakecase
from shared_utils import arcgis_query

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/shared_data/"
COMPILED_CACHED_GCS = "gs://calitp-analytics-data/data-analyses/rt_delay/compiled_cached_views/"


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

    # Save a raw, undissolved version
    utils.geoparquet_gcs_export(
        gdf.drop(columns=["Shape_Length", "OBJECTID"]).pipe(to_snakecase), GCS_FILE_PATH, "state_highway_network_raw"
    )

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

    gdf = arcgis_query.query_arcgis_feature_server(URL)

    gdf2 = to_snakecase(gdf).drop(columns="objectid")

    utils.geoparquet_gcs_export(gdf2, GCS_FILE_PATH, "state_highway_network_postmiles")

    return


def scaled_proportion(x: float, odometer_min: float, odometer_max: float) -> float:
    """
    Scale highway postmile start and end to 0-1.
    Ex: if a highway line has odometer value of 0 - 1_000,
    and the postmile segment we want to cut is 400-600, we need to know
    proportionally which subset of coords to take.
    Use this along with shapely.project to find out the distance
    each coord is from the origin.
    """
    return (x - odometer_min) / (odometer_max - odometer_min)


def get_segment_geom(
    shn_line_geom: shapely.LineString, projected_coords: list, start_dist: float, end_dist: float
) -> shapely.LineString:
    """
    Unpack SHN linestring coordinates as distances from origin
    of linestring, and find where postmiles start/end.
    The segment we're interested in is this line:
    (postmile start point, coordinates on SHN line in between, and postmile end point).
    """
    # Turn list into array so we can subset it
    projected_coords = np.asarray(projected_coords)

    # The postmile's bodometer is start_dist; postmile's eodometer is end_dist
    # Convert these to point geometries (these are our vertices)
    start_geom = shn_line_geom.interpolate(start_dist, normalized=True)
    end_geom = shn_line_geom.interpolate(end_dist, normalized=True)

    # Valid indices are all the coords in the SHN line that are between the 2 postmiles
    valid_indices = ((projected_coords >= start_dist) & (projected_coords <= end_dist)).nonzero()[0]
    valid_coords = list(np.asarray([shapely.Point(p) for p in shn_line_geom.coords])[valid_indices])

    # Create our segment based on our postmile start/end points + all the coordinates in between
    # found on the SHN linestring
    segment_geom = shapely.LineString([start_geom, *valid_coords, end_geom])

    return segment_geom


def segment_highway_lines_by_postmile(gdf: gpd.GeoDataFrame):
    """
    Use the current postmile as the
    starting geometry / segment beginning
    and the subsequent postmile (based on odometer)
    as the ending geometry / segment end.

    Segment goes from current to next postmile.
    """
    # Take the postmile's bodometer/eodometer and
    # find the scaled version relative to the highway's bodometer/eodometer.
    # hwy_bodometer/eodometer usually are floats like [3.5, 4.8], and we want to know
    # proportionally where a value like 4.2 will fall on a scale of 0-1
    start_dist = np.vectorize(scaled_proportion)(gdf.bodometer, gdf.hwy_bodometer, gdf.hwy_eodometer)
    end_dist = np.vectorize(scaled_proportion)(gdf.eodometer, gdf.hwy_bodometer, gdf.hwy_eodometer)

    segment_geom = np.vectorize(get_segment_geom)(gdf.line_geometry, gdf.projected_coords, start_dist, end_dist)

    drop_cols = ["projected_coords", "hwy_bodometer", "hwy_eodometer", "line_geometry"]

    # Assign segment geometry and overwrite the postmile geometry column
    gdf2 = (
        gdf.assign(geometry=gpd.GeoSeries(segment_geom, crs=geography_utils.CA_NAD83Albers))
        .drop(columns=drop_cols)
        .set_geometry("geometry")
    )

    return gdf2


def round_odometer_values(df: pd.DataFrame, cols: list = ["odometer"], num_decimals: int = 3) -> pd.DataFrame:
    """
    Round odometer columns, which can be named
    odometer, bodometer (begin odometer), eodometer (end odometer)
    to 3 decimal places.
    """
    df[cols] = df[cols].round(num_decimals)

    return df


def create_postmile_segments(
    group_cols: list = ["county", "routetype", "route", "direction", "routes", "pmrouteid"]
) -> gpd.GeoDataFrame:
    """
    Take the SHN postmiles gdf, group by highway / odometer
    and convert the points into lines.
    We'll lose the last postmile for each highway-direction.
    Segment goes from current postmile point to subseq postmile point.
    """
    # We need multilinestrings to become linestrings (use gdf.explode)
    # and the columns we select do uniquely tag lines (multilinestrings are 1 item)
    hwy_lines = (
        gpd.read_parquet(
            f"{GCS_FILE_PATH}state_highway_network_raw.parquet",
            columns=group_cols + ["bodometer", "eodometer", "geometry"],
        )
        .explode("geometry")
        .reset_index(drop=True)
        .pipe(round_odometer_values, ["bodometer", "eodometer"], num_decimals=3)
        .to_crs(geography_utils.CA_NAD83Albers)
    )

    # Have a list accompany the geometry
    # linestring has many coords, shapely.project each point and calculate distance from origin
    # and normalize it all between 0-1
    hwy_lines = hwy_lines.assign(
        projected_coords=hwy_lines.apply(
            lambda x: [x.geometry.project(shapely.Point(p), normalized=True) for p in x.geometry.coords], axis=1
        )
    )

    hwy_postmiles = (
        gpd.read_parquet(
            f"{GCS_FILE_PATH}state_highway_network_postmiles.parquet", columns=group_cols + ["odometer", "geometry"]
        )
        .pipe(round_odometer_values, ["odometer"], num_decimals=3)
        .to_crs(geography_utils.CA_NAD83Albers)
    )
    # Round to 3 digits for odometer. When there are more decimal places, it makes our cutoffs iffy
    # when we use this condition below: odometer >= bodometer & odometer <= eodometer

    # follow the convention of b for begin odometer and e for end odometer
    hwy_postmiles = (
        hwy_postmiles.assign(
            eodometer=(hwy_postmiles.sort_values(group_cols + ["odometer"]).groupby(group_cols).odometer.shift(-1)),
        )
        .rename(columns={"odometer": "bodometer"})
        .dropna(subset="eodometer")
        .reset_index(drop=True)
    )

    # Merge hwy points with the lines we want to cut segments from
    gdf = (
        pd.merge(
            hwy_postmiles,
            hwy_lines.rename(
                columns={"bodometer": "hwy_bodometer", "eodometer": "hwy_eodometer", "geometry": "line_geometry"}
            ),
            on=group_cols,
            how="inner",
        )
        .query(
            # make sure that the postmile point falls between
            # the beginning and ending odometer
            "bodometer >= hwy_bodometer & eodometer <= hwy_eodometer"
        )
        .sort_values(group_cols + ["bodometer"])
        .reset_index(drop=True)
    )

    gdf2 = segment_highway_lines_by_postmile(gdf).to_crs(geography_utils.WGS84)

    utils.geoparquet_gcs_export(gdf2, GCS_FILE_PATH, "state_highway_network_postmile_segments")

    return


def export_combined_legislative_districts() -> gpd.GeoDataFrame:
    """
    Create a combined assembly district and senate districts
    gdf.
    """
    BASE_URL = "https://services3.arcgis.com/fdvHcZVgB2QSRNkL/" "arcgis/rest/services/Legislative/FeatureServer/"
    SUFFIX = "query?outFields=*&where=1%3D1&f=geojson"

    ASSEMBLY_DISTRICTS = f"{BASE_URL}0/{SUFFIX}"
    SENATE_DISTRICTS = f"{BASE_URL}1/{SUFFIX}"

    ad = gpd.read_file(ASSEMBLY_DISTRICTS)
    sd = gpd.read_file(SENATE_DISTRICTS)

    gdf = pd.concat(
        [
            ad[["AssemblyDistrictLabel", "geometry"]].rename(columns={"AssemblyDistrictLabel": "legislative_district"}),
            sd[["SenateDistrictLabel", "geometry"]].rename(columns={"SenateDistrictLabel": "legislative_district"}),
        ],
        axis=0,
        ignore_index=True,
    )

    utils.geoparquet_gcs_export(gdf, GCS_FILE_PATH, "legislative_districts")
    return


def sjoin_shapes_legislative_districts(analysis_date: str) -> pd.DataFrame:
    """
    Grab shapes for a single day and do a spatial join
    with legislative district.
    Keep 1 row for every operator-legislative_district combination.
    """
    operator_cols = ["name"]
    # keeping gtfs_dataset_key gets us duplicate rows by name,
    # and by the time we're filtering in GTFS digest, we already have name attached

    operator_shapes = pd.read_parquet(
        f"{COMPILED_CACHED_GCS}trips_{analysis_date}.parquet", columns=operator_cols + ["shape_array_key"]
    ).drop_duplicates()

    shapes = gpd.read_parquet(
        f"{COMPILED_CACHED_GCS}routelines_{analysis_date}.parquet", columns=["shape_array_key", "geometry"]
    )

    legislative_districts = gpd.read_parquet(f"{GCS_FILE_PATH}legislative_districts.parquet")

    gdf = pd.merge(shapes, operator_shapes, on="shape_array_key", how="inner").drop(columns="shape_array_key")

    crosswalk = gpd.sjoin(gdf, legislative_districts, how="inner", predicate="intersects")[
        operator_cols + ["legislative_district"]
    ].drop_duplicates()

    return crosswalk


def make_transit_operators_to_legislative_district_crosswalk(date_list: list) -> pd.DataFrame:
    """
    Put all the dates we have from Mar 2023 - Sep 2024
    and get a main crosswalk for operators to legislative districts.
    Over time, we can rerun this and update our crosswalk.
    """
    gdf = (
        pd.concat([sjoin_shapes_legislative_districts(d) for d in date_list], axis=0, ignore_index=True)
        .drop_duplicates()
        .sort_values(["name", "legislative_district"])
        .reset_index(drop=True)
    )

    gdf.to_parquet(f"{GCS_FILE_PATH}" "crosswalk_transit_operators_legislative_districts.parquet")

    return


if __name__ == "__main__":
    # Run functions to create these datasets...store in GCS
    from shared_utils import rt_dates

    # CA counties
    make_county_centroids()

    # State Highway Network
    make_clean_state_highway_network()
    export_shn_postmiles()

    # This takes 24 min to run, so if there's a way to optimize in the future, we should
    create_postmile_segments(["district", "county", "routetype", "route", "direction", "routes", "pmrouteid"])

    # Legislative Districts
    export_combined_legislative_districts()

    make_transit_operators_to_legislative_district_crosswalk(rt_dates.y2024_dates + rt_dates.y2023_dates)
