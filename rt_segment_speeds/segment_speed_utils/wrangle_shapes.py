"""
Functions for applying shapely project and interpolation.
Move our shapes (linestrings) and stops (points) from coordinates
to numpy arrays with numeric values (shape_meters) and vice versa.
"""
import dask.array as da
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from typing import Literal

from shared_utils import rt_utils
from segment_speed_utils.project_vars import PROJECT_CRS


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


def project_point_geom_onto_linestring(
    shape_geoseries: gpd.GeoSeries,
    point_geoseries: gpd.GeoSeries,
    get_dask_array: bool = True
) -> pd.Series:
    """
    Use shapely.project to turn point coordinates into numeric.
    The point coordinates will be converted to the distance along the linestring.
    https://shapely.readthedocs.io/en/stable/manual.html?highlight=project#object.project
    https://gis.stackexchange.com/questions/306838/snap-points-shapefile-to-line-shapefile-using-shapely
    """
    if shape_geoseries.crs != point_geoseries.crs:
        error_msg = (
            f"CRS do not match: shape CRS is {shape_geoseries.crs.to_epsg()}" 
            f"and point CRS is {point_geoseries.crs.to_epsg()}"
        )
        raise AttributeError(error_msg)
    
    shape_meters_geoseries = shape_geoseries.project(point_geoseries)
    
    if get_dask_array:
        shape_meters_geoseries = da.array(shape_meters_geoseries)
    
    # To add this as a column to a dask df
    # https://www.appsloveworld.com/coding/dataframe/6/add-a-dask-array-column-to-a-dask-dataframe
    return shape_meters_geoseries


def linear_reference_vp_against_segment(
    vp: dd.DataFrame, 
    segments: gpd.GeoDataFrame, 
    segment_identifier_cols: list,
) -> dd.DataFrame:
    """
    Do linear referencing and calculate `shape_meters` for the 
    enter/exit points on the segment. 
    
    From Eric: projecting the stop's point geom onto the shape_id's line geom
    https://github.com/cal-itp/data-analyses/blob/f4c9c3607069da6ea96e70c485d0ffe1af6d7a47/rt_delay/rt_analysis/rt_parser.py#L102-L103
    
    This allows us to calculate the distance_elapsed.
    
    Return dd.DataFrame because geometry makes the file huge.
    Merge in segment geometry later before plotting.
    """
    
    if isinstance(segments, dg.GeoDataFrame):
        segments = segments.compute()
    
    if isinstance(vp, pd.DataFrame):
        vp = dd.from_pandas(vp, npartitions=1)
    
    # https://stackoverflow.com/questions/71685387/faster-methods-to-create-geodataframe-from-a-dask-or-pandas-dataframe
    # https://github.com/geopandas/dask-geopandas/issues/197
    vp["geometry"] = dg.points_from_xy(vp, "x", "y")
    
    # Refer to the geometry column by name
    vp_gddf = dg.from_dask_dataframe(
        vp, 
        geometry="geometry"
    ).set_crs("EPSG:4326").to_crs(PROJECT_CRS)
    
    linear_ref_vp_to_shape = dd.merge(
        vp_gddf, 
        segments[segments.geometry.notna()
                ][segment_identifier_cols + ["geometry"]],
        on = segment_identifier_cols,
        how = "inner"
    )
    
    # Convert to geoseries so that we can do the project
    vp_geoseries = gpd.GeoSeries(linear_ref_vp_to_shape.geometry_x.compute())
    shape_geoseries = gpd.GeoSeries(linear_ref_vp_to_shape.geometry_y.compute())
    
    # Project, save results, then convert to dask array, 
    # otherwise can't add a column to the dask df    
    shape_meters_geoseries = project_point_geom_onto_linestring(
        shape_geoseries,
        vp_geoseries,
        get_dask_array=True
    )

    # https://www.appsloveworld.com/coding/dataframe/6/add-a-dask-array-column-to-a-dask-dataframe
    linear_ref_vp_to_shape['shape_meters'] = shape_meters_geoseries
    
    linear_ref_df = (linear_ref_vp_to_shape.drop(
        columns = ["geometry_x", "geometry_y",
                   "x", "y"])
                .drop_duplicates()
                .reset_index(drop=True)
    )
    
    return linear_ref_df


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