"""
Functions for applying shapely project and interpolation.
Move our shapes (linestrings) and stops (points) from coordinates
to numpy arrays with numeric values (shape_meters) and vice versa.

References:
* Tried method 4: https://gis.stackexchange.com/questions/203048/split-lines-at-points-using-shapely -- debug because we lost curves
* https://stackoverflow.com/questions/31072945/shapely-cut-a-piece-from-a-linestring-at-two-cutting-points
* https://gis.stackexchange.com/questions/210220/break-a-shapely-linestring-at-multiple-points
* https://gis.stackexchange.com/questions/416284/splitting-multiline-or-linestring-into-equal-segments-of-particular-length-using
* https://stackoverflow.com/questions/62053253/how-to-split-a-linestring-to-segments
"""
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from typing import Literal

from calitp_data_analysis import geography_utils
from shared_utils import rt_utils
from segment_speed_utils.project_vars import PROJECT_CRS

ALL_DIRECTIONS = ["Northbound", "Southbound", "Eastbound", "Westbound"]

OPPOSITE_DIRECTIONS = {
    "Northbound": "Southbound",
    "Southbound": "Northbound",
    "Eastbound": "Westbound",
    "Westbound": "Eastbound",
    "Unknown": "",
}

def interpolate_projected_points(
    shape_geometry: shapely.geometry.LineString,
    projected_list: list
):
    return [shape_geometry.interpolate(i) for i in projected_list]


def project_list_of_coords(
    shape_geometry: shapely.geometry.LineString,
    point_geom_list: list = [],
    use_shapely_coords: bool = False
) -> np.ndarray:
    if use_shapely_coords:
        # https://stackoverflow.com/questions/49330030/remove-a-duplicate-point-from-polygon-in-shapely
        # use simplify(0) to remove any points that might be duplicates
        return np.asarray(
            [shape_geometry.project(shapely.geometry.Point(p))
            for p in shape_geometry.simplify(0).coords])
    else:
        return np.asarray(
            [shape_geometry.project(i) for i in point_geom_list])

    
def add_arrowized_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add a column where the segment is arrowized.
    """

    segment_geom = gpd.GeoSeries(gdf.geometry)
    CRS = gdf.crs.to_epsg()
    
    #TODO: parallel_offset is going to be deprecated? offset_curve is the new one
    geom_parallel = gpd.GeoSeries(
        [rt_utils.try_parallel(i) for i in segment_geom], crs=CRS)
    #geom_parallel = gpd.GeoSeries(
    #    [i.offset_curve(30) for i in segment_geom], 
    #    crs=CRS
    #)
    
    geom_arrowized = rt_utils.arrowize_segment(
        geom_parallel, 
        buffer_distance = 20
    )
    
    gdf = gdf.assign(
        geometry_arrowized = geom_arrowized
    )

    return gdf


def array_to_geoseries(
    array: np.ndarray,
    geom_type: Literal["point", "line", "polygon"],
    crs: str = "EPSG:3310"
)-> gpd.GeoSeries: 
    """
    Turn array back into geoseries.
    """
    if geom_type == "point":
        gdf = gpd.GeoSeries(array, crs=crs)
        
    elif geom_type == "line":
        gdf = gpd.GeoSeries(
            shapely.geometry.LineString(array), 
            crs=crs)
        
    elif geom_type == "polygon":
        gdf = gpd.GeoSeries(
            shapely.geometry.Polygon(array),
            crs = crs)
    
    return gdf


def get_direction_vector(
    start: shapely.geometry.Point, 
    end: shapely.geometry.Point
) -> tuple:
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
    return np.sqrt(vector[0]**2 + vector[1]**2)


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
    return vec1[0]*vec2[0] + vec1[1]*vec2[1]


def vp_as_gdf(
    vp: pd.DataFrame,
    crs: str = PROJECT_CRS
) -> gpd.GeoDataFrame:
    """
    Turn vp as a gdf and project to EPSG:3310.
    """
    vp_gdf = geography_utils.create_point_geometry(
        vp,
        longitude_col = "x", latitude_col = "y",
        crs = geography_utils.WGS84
    ).to_crs(crs).drop(columns = ["x", "y"])
        
    return vp_gdf


def interpolate_stop_arrival_time(
    stop_position: float, 
    shape_meters_arr: np.ndarray,
    timestamp_arr: np.ndarray
) -> float:
    """
    Interpolate the arrival time given the stop meters position.
    Cast datetimes into floats and cast back as datetime.
    """
    timestamp_arr = np.asarray(timestamp_arr).astype("datetime64[s]").astype("float64")

    return np.interp(
        stop_position, np.asarray(shape_meters_arr), timestamp_arr
    ).astype("datetime64[s]")