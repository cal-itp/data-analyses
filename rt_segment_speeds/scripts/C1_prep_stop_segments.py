"""
Stop-to-stop segments by shape_id.

gtfs_schedule.01_stop_route_table.ipynb
shows that stop_sequence would probably be unique at shape_id level, but
not anything more aggregated than that (not route-direction).

References:
* Used this one (method 4): https://gis.stackexchange.com/questions/203048/split-lines-at-points-using-shapely
* https://stackoverflow.com/questions/31072945/shapely-cut-a-piece-from-a-linestring-at-two-cutting-points
* https://gis.stackexchange.com/questions/210220/break-a-shapely-linestring-at-multiple-points
* https://gis.stackexchange.com/questions/416284/splitting-multiline-or-linestring-into-equal-segments-of-particular-length-using
* https://stackoverflow.com/questions/62053253/how-to-split-a-linestring-to-segments
"""
import datetime
import geopandas as gpd
import pandas as pd
import shapely
import sys

from loguru import logger

from update_vars import SEGMENT_GCS, COMPILED_CACHED_VIEWS, analysis_date


def attach_shape_id_to_stop_times(analysis_date: str) -> pd.DataFrame:
    """
    Merge stop_times and trips to get shape_id.
    
    Keep unique stop_id-stop_sequence combo at shape-level. 
    """
    stop_times = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}st_{analysis_date}.parquet",
        columns = ["feed_key", "trip_id", "stop_id", "stop_sequence"]
    )
    
    trips = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet", 
        columns = ["feed_key", "trip_id", "shape_id", "shape_array_key"]
    )
    
    st_with_shape = pd.merge(
        stop_times, 
        trips,
        on = ["feed_key", "trip_id"],
        how = "inner",
        validate = "m:1"
    )
    
    st_by_shape = (st_with_shape.drop_duplicates(
        subset=["feed_key", "shape_id", 
                "stop_id", "stop_sequence"]
         ).drop(columns = "trip_id")
        .sort_values(["feed_key", "shape_id", "stop_sequence"])
        .reset_index(drop=True)
    )
    
    return st_by_shape


def stop_times_with_stop_geom(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Attach stop's point geometry to stop_times table.
    Use trips in between to get it attached via shape_id.
    
    Keep stop_time's stops at the `shape_id` level.
    """
    stop_times = attach_shape_id_to_stop_times(analysis_date)
    
    stops = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet",
        columns = ["feed_key", "stop_id", "stop_name", "geometry"]
    ).drop_duplicates(subset=["feed_key", "stop_id"]).reset_index(drop=True)
    
    gdf = pd.merge(
        stops, 
        stop_times,
        on = ["feed_key", "stop_id"],
        how = "inner",
        validate = "1:m"
    ).sort_values(["feed_key", "shape_id", 
                   "stop_sequence"]
                 ).drop_duplicates(
        subset=["feed_key", "shape_id", "stop_id"]
    ).reset_index(drop=True)
    
    return gdf


def merge_in_shape_geom_and_project(
    stops: gpd.GeoDataFrame, 
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Merge in shape's line geometry.
    shapely.project(x) returns the distance along the line geometry
    nearest the stop point geometry.
    
    From Eric: projecting the stop's point geom onto the shape_id's line geom
    https://github.com/cal-itp/data-analyses/blob/f4c9c3607069da6ea96e70c485d0ffe1af6d7a47/rt_delay/rt_analysis/rt_parser.py#L102-L103
    """
    shapes = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet", 
        columns = ["shape_array_key", "geometry"]
    )
    
    stops_with_shape = pd.merge(
        stops,
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = ["shape_array_key"],
        how = "inner",
        validate = "m:1"
    )
        
    # Once we merge in shape's line geometry, we can do the project
    # with itertuples, since shapely does it element by element
    # https://gis.stackexchange.com/questions/306838/snap-points-shapefile-to-line-shapefile-using-shapely
    projected = []
    #interpolated = []
    
    for row in stops_with_shape.itertuples():
        row_shape_geom = getattr(row, "shape_geometry")
        row_stop_geom = getattr(row, "geometry")
        
        point_projected_along_shape = row_shape_geom.project(row_stop_geom)
        projected.append(point_projected_along_shape)
        
        #point_projected_and_interpolated_along_shape = row_shape_geom.interpolate(
        #    row_shape_geom.project(row_stop_geom))
        #interpolated.append(point_projected_and_interpolated_along_shape)
    
    #shape_meters_x = [shapely.geometry.Point(i).x for i in interpolated]
    #shape_meters_y = [shapely.geometry.Point(i).y for i in interpolated]
    
    stops_with_shape = stops_with_shape.assign(
        shape_meters = projected,
        #stop_interpolated = gpd.points_from_xy(shape_meters_x, 
        #                                       shape_meters_y,
        #                                       crs = "EPSG:3310")
    )
        
    return stops_with_shape


def make_shape_meters_wide(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    shape_cols = ["shape_array_key"]
    
    unique_shapes = (gdf.set_geometry("shape_geometry")
                     [shape_cols + ["shape_geometry"]]
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

    logger.add("../logs/C1_prep_stop_segments.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    stops = stop_times_with_stop_geom(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"add stop geom to stop_times: {time1-start}")
    
    stops_projected = merge_in_shape_geom_and_project(
        stops, analysis_date)
    stops_projected.to_parquet("./data/stops_projected.parquet")

    time2 = datetime.datetime.now()
    logger.info(f"linear referencing of stops to the shape's line_geom: {time2-time1}")
    
    # Turn df from long to wide (just 1 row per shape_array_key)
    stops_projected_wide = make_shape_meters_wide(stops_projected)
    stops_projected_wide.to_parquet("./data/stops_projected_wide.parquet")    
       
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    