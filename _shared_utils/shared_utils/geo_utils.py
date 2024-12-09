"""
Geospatial utility functions
"""
from typing import Union

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from calitp_data_analysis import geography_utils
from scipy.spatial import KDTree
from shared_utils import rt_utils

# Could we use distance to filter for nearest neighbor?
# It can make the length of results more unpredictable...maybe we stick to
# k_neighbors and keep the nearest k, so that we can at least be
# more consistent with the arrays returned
geo_const_meters = 6_371_000 * np.pi / 180
geo_const_miles = 3_959_000 * np.pi / 180


def nearest_snap(line: Union[shapely.LineString, np.ndarray], point: shapely.Point, k_neighbors: int = 1) -> np.ndarray:
    """
    Based off of this function,
    but we want to return the index value, rather than the point.
    https://github.com/UTEL-UIUC/gtfs_segments/blob/main/gtfs_segments/geom_utils.py
    """
    if isinstance(line, shapely.LineString):
        line = np.asarray(line.coords)
    elif isinstance(line, np.ndarray):
        line = line
    point = np.asarray(point.coords)
    tree = KDTree(line)

    # np_dist is array of distances of result (let's not return it)
    # np_inds is array of indices of result
    _, np_inds = tree.query(
        point,
        workers=-1,
        k=k_neighbors,
    )

    return np_inds.squeeze()


def vp_as_gdf(vp: pd.DataFrame, crs: str = "EPSG:3310") -> gpd.GeoDataFrame:
    """
    Turn vp as a gdf and project to EPSG:3310.
    """
    vp_gdf = (
        geography_utils.create_point_geometry(vp, longitude_col="x", latitude_col="y", crs=geography_utils.WGS84)
        .to_crs(crs)
        .drop(columns=["x", "y"])
    )

    return vp_gdf


def add_arrowized_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add a column where the segment is arrowized.
    """
    segment_geom = gpd.GeoSeries(gdf.geometry)
    CRS = gdf.crs.to_epsg()

    # TODO: parallel_offset is going to be deprecated? offset_curve is the new one
    geom_parallel = gpd.GeoSeries([rt_utils.try_parallel(i) for i in segment_geom], crs=CRS)
    # geom_parallel = gpd.GeoSeries(
    #    [i.offset_curve(30) for i in segment_geom],
    #    crs=CRS
    # )

    geom_arrowized = rt_utils.arrowize_segment(geom_parallel, buffer_distance=20)

    gdf = gdf.assign(geometry_arrowized=geom_arrowized)

    return gdf


def get_direction_vector(start: shapely.geometry.Point, end: shapely.geometry.Point) -> tuple:
    """
    Given 2 points (in a projected CRS...not WGS84), return a
    tuple that shows (delta_x, delta_y).

    https://www.varsitytutors.com/precalculus-help/find-a-direction-vector-when-given-two-points
    https://stackoverflow.com/questions/17332759/finding-vectors-with-2-points

    """
    return ((end.x - start.x), (end.y - start.y))


def distill_array_into_direction_vector(array: np.ndarray) -> tuple:
    """
    Given an array of n items, let's take the start/end of that.
    From start/end, we can turn 2 coordinate points into 1 distance vector.
    Distance vector is a tuple that equals (delta_x, delta_y).
    """
    origin = array[0]
    destination = array[-1]
    return get_direction_vector(origin, destination)


def get_vector_norm(vector: tuple) -> float:
    """
    Get the length (off of Pythagorean Theorem) by summing up
    the squares of the components and then taking square root.

    Use Pythagorean Theorem to get unit vector. Divide the vector
    by the length of the vector to get unit/normalized vector.
    This equation tells us what we need to divide by.
    """
    return np.sqrt(vector[0] ** 2 + vector[1] ** 2)


def get_normalized_vector(vector: tuple) -> tuple:
    """
    Apply Pythagorean Theorem and normalize the vector of distances.
    https://stackoverflow.com/questions/21030391/how-to-normalize-a-numpy-array-to-a-unit-vector
    """
    x_norm = vector[0] / get_vector_norm(vector)
    y_norm = vector[1] / get_vector_norm(vector)

    return (x_norm, y_norm)


def dot_product(vec1: tuple, vec2: tuple) -> float:
    """
    Take the dot product. Multiply the x components, the y components, and
    sum it up.
    """
    return vec1[0] * vec2[0] + vec1[1] * vec2[1]


def segmentize_by_indices(line_geometry: shapely.LineString, start_idx: int, end_idx: int) -> shapely.LineString:
    """
    Cut a line according to index values.
    Similar to shapely.segmentize, which allows you to cut
    line according to distances.
    Here, we don't have specified distances, but we want to customize
    where segment the line.
    """
    all_coords = shapely.get_coordinates(line_geometry)

    if end_idx + 1 > all_coords.size:
        subset_coords = all_coords[start_idx:end_idx]
    else:
        subset_coords = all_coords[start_idx : end_idx + 1]

    if len(subset_coords) < 2:
        return shapely.LineString()
    else:
        return shapely.LineString([shapely.Point(i) for i in subset_coords])


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
