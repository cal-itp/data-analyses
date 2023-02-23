"""
Prep dfs to cut stop-to-stop segments by shape_id.

Use np.arrays and store shape_geometry as meters from origin.
An individual stop-to-stop segment has starting point of previous stop's projected coord and end point at current stop's projected coord.
Also add in the shape's coords present (which adds more detail, including curves).

gtfs_schedule.01_stop_route_table.ipynb
shows that stop_sequence would probably be unique at shape_id level, but
not anything more aggregated than that (not route-direction).

References:
* Tried method 4: https://gis.stackexchange.com/questions/203048/split-lines-at-points-using-shapely -- debug because we lost curves
* https://stackoverflow.com/questions/31072945/shapely-cut-a-piece-from-a-linestring-at-two-cutting-points
* https://gis.stackexchange.com/questions/210220/break-a-shapely-linestring-at-multiple-points
* https://gis.stackexchange.com/questions/416284/splitting-multiline-or-linestring-into-equal-segments-of-particular-length-using
* https://stackoverflow.com/questions/62053253/how-to-split-a-linestring-to-segments
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import shapely
import sys

from loguru import logger

from shared_utils import utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, analysis_date


def stop_times_aggregated_to_shape_array_key(
    analysis_date: str
) -> dg.GeoDataFrame:
    """
    For stop-to-stop segments, we need to aggregate stop_times,
    which comes at trip-level, to shape level. 
    From trips, attach shape_array_key, then merge to stop_times.
    Then attach stop's point geom.
    """
    
    shapes = helpers.import_scheduled_shapes(analysis_date)

    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "name", "trip_id", "shape_array_key"]
    )

    trips_with_geom = gtfs_schedule_wrangling.merge_shapes_to_trips(shapes, trips)

    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "trip_id", "stop_id", "stop_sequence"]
    )

    # Attach shapes 
    stop_times_with_shape = gtfs_schedule_wrangling.merge_shapes_to_stop_times(
        trips_with_geom,
        stop_times
    )

    # For each shape_array_key, keep the unique stop_id-stop_sequence combo
    # to cut into stop-to-stop segments
    stop_times_with_shape = (stop_times_with_shape.drop_duplicates(
        subset=["shape_array_key", 
                "stop_id", "stop_sequence"]
         ).drop(columns = "trip_id")
        .sort_values(["shape_array_key", "stop_sequence"])
        .reset_index(drop=True)
    )
    
    # Attach stop geom
    stops = helpers.import_scheduled_stops(
        analysis_date,
        columns = ["feed_key", "stop_id", "stop_name", "geometry"]
    )
    
    # Renaming geometry column before causes error, so rename after the merge
    st_with_shape_stop_geom = gtfs_schedule_wrangling.attach_stop_geometry(
        stop_times_with_shape,
        stops,
    ).rename(columns = {
        "geometry_x": "geometry", 
        "geometry_y": "stop_geometry"
    }).set_geometry("geometry")

    
    return st_with_shape_stop_geom


def prep_stop_segments(analysis_date: str) -> dg.GeoDataFrame:
    stop_times_with_geom = stop_times_aggregated_to_shape_array_key(analysis_date)
    
    # Turn the stop_geometry and shape_geometry columns into geoseries
    shape_geoseries = gpd.GeoSeries(stop_times_with_geom.geometry.compute())
    stop_geoseries = gpd.GeoSeries(stop_times_with_geom.stop_geometry.compute())
    
    # Get projected shape_meters as dask array
    shape_meters_geoseries = wrangle_shapes.project_point_geom_onto_linestring(
        shape_geoseries,
        stop_geoseries,
        get_dask_array=True
    )
    
    # Attach dask array as a column
    stop_times_with_geom["shape_meters"] = shape_meters_geoseries
    
    return stop_times_with_geom


def transform_wide_by_shape_array_key(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Make the gdf wide so that we can save certain columns as arrays or lists.
    """
    shape_cols = ["shape_array_key"]
    
    unique_shapes = (gdf
                     [shape_cols + ["geometry"]]
                     .drop_duplicates()
                    )
    
    gdf_wide = (gdf.groupby(shape_cols)
                .agg({
                    "shape_meters": lambda x: list(x), 
                    "stop_sequence": lambda x: list(x)
                }).reset_index()
               )
    
    gdf_wide2 = pd.merge(
        unique_shapes,
        gdf_wide,
        on = shape_cols,
        how = "inner"
    )
    
    return gdf_wide2


if __name__=="__main__":
    import warnings
    
    warnings.filterwarnings(
        "ignore",
        category=shapely.errors.ShapelyDeprecationWarning) 

    LOG_FILE = "../logs/prep_stop_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    stops_by_shape = prep_stop_segments(analysis_date).compute()
    
    time1 = datetime.datetime.now()
    logger.info(f"Prep stop segment df: {time1-start}")
    
    utils.geoparquet_gcs_export(
        stops_by_shape,
        SEGMENT_GCS,
        f"stops_projected_{analysis_date}"
    )
    
    # Turn df from long to wide (just 1 row per shape_array_key)
    stops_by_shape_wide = transform_wide_by_shape_array_key(stops_by_shape)
    
    time2 = datetime.datetime.now()
    logger.info(f"Make stop segment wide: {time2-time1}")
    
    utils.geoparquet_gcs_export(
        stops_by_shape_wide,
        SEGMENT_GCS,
        f"stops_projected_wide_{analysis_date}"
    )
       
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    