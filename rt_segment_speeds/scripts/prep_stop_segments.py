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
import os
os.environ['USE_PYGEOS'] = '0'

import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from loguru import logger
from typing import Union

from shared_utils import utils
from segment_speed_utils import (helpers, gtfs_schedule_wrangling,
                                 wrangle_shapes)
from segment_speed_utils.project_vars import SEGMENT_GCS, analysis_date


def stop_times_aggregated_to_shape_array_key(
    analysis_date: str,
) -> dg.GeoDataFrame:
    """
    For stop-to-stop segments, we need to aggregate stop_times,
    which comes at trip-level, to shape level. 
    From trips, attach shape_array_key, then merge to stop_times.
    Then attach stop's point geom.
    """
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "name", 
                    "trip_id", "shape_array_key"],
        get_pandas = True
    )

    trips = gtfs_schedule_wrangling.exclude_scheduled_operators(
        trips, 
        exclude_me = ["Amtrak Schedule", "*Flex"]
    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date, 
        columns = ["shape_array_key", "geometry"],
        get_pandas = False,
    ).dropna(subset="shape_array_key").repartition(npartitions=5)

    stops = helpers.import_scheduled_stops(
        analysis_date,
        columns = ["feed_key", "stop_id", "stop_name", "geometry"],
        get_pandas = True
    ).drop_duplicates(
        subset=["feed_key", "stop_id"]
    ).rename(columns = {"geometry": "stop_geometry"}
            ).set_geometry("stop_geometry")

    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "trip_id", "stop_id", "stop_sequence"]
    ).astype({"stop_sequence": "int16"}).repartition(npartitions=20)
    

    # For each shape_array_key, keep the unique stop_id-stop_sequence combo
    # to cut into stop-to-stop segments
    stop_times_with_shape = dd.merge(
        stop_times,
        trips,
        on = ["feed_key", "trip_id"],
        how = "inner"
    ).drop(columns = "trip_id").drop_duplicates(
        subset=["shape_array_key", "stop_id", "stop_sequence"]
    ).compute().sort_values(
        ["shape_array_key", "stop_sequence"]
    ).reset_index(drop=True)
    
    # Attach shape geom
    st_with_shape = dd.merge(
        shapes,
        stop_times_with_shape,
        on = "shape_array_key",
        how = "inner"
    )
    
    # Attach stop geom
    st_with_shape_stop_geom = dd.merge(
        st_with_shape,
        stops,
        on = ["feed_key", "stop_id"],
        how = "inner"
    ).set_geometry("geometry")
    
    st_with_shape_stop_geom = st_with_shape_stop_geom.dropna(
        subset="geometry").reset_index(drop=True).persist()
    
    return st_with_shape_stop_geom


def tag_shapes_with_stops_visited_twice(
    stop_times: Union[dd.DataFrame, dg.GeoDataFrame]
) -> np.ndarray:
    """
    Aggregate stop times by shape_array_key.
    For each stop_id, count how many stop_sequence values there are. 
    More than 1 means that the same stop_id is revisited on the same trip.
    
    Ex: a stop in a plaza that acts as origin and destination.
    """
    stop_visits = (stop_times.groupby(
                    ["shape_array_key", "stop_id"], observed=True, group_keys=False)
                  .agg({"stop_sequence": "count"}) 
                   #nunique doesn't work in dask
                  .reset_index()
                 )
    
    
    # If any of the trips within that shape revisits a stop, keep that shape
    loopy_shapes = (stop_visits[stop_visits.stop_sequence > 1]
                    .shape_array_key
                    .unique()
                    .compute()
                    .to_numpy()
                 )
    
    return loopy_shapes


def tag_shapes_with_inlining(
    stop_times: Union[dd.DataFrame, dg.GeoDataFrame]
) -> np.ndarray:
    """
    Rough estimate of inlining present in shapes. 
    Based on stops projected onto the shape_geometry, tag any 
    shapes where the stops are not monotonically increasing.
    When stops are converted from coordinates to distances (shapely.project), 
    it doesn't project neatly monotonically in the section of inlining, 
    because a stop can be projected onto several coord options on the shape.
    Any weirdness of values jumping around means we want to cut it with 
    super_project.
    """
    # Keep relevant columns, which is only the projected stop geometry
    # saved as shape_meters
    stop_times2 = stop_times[["shape_array_key", 
                              "stop_sequence", "shape_meters"]].compute()
    
    # Once we order it by stop sequence, save out shape_meters into a list
    # and make the gdf wide
    stop_times_wide = (stop_times2
                       .sort_values(["shape_array_key", "stop_sequence"])
                       .groupby("shape_array_key", observed=True, group_keys=False)
                       .agg({"shape_meters": lambda x: list(x)})
                       .reset_index()
                      )
    
    # Once it's wide, we can check whether the array in each row is 
    # monotonically increasing. If it's not, it's because the stop's projection 
    # as shape_meters is jumping wildly, which could indicate there's inlining present
    # first take the difference from prior value in the array
    # if it's monotonically increasing, the difference is always positive. any negative 
    # values indicates the value is fluctating.
    is_monotonic = [
        np.all(np.diff(shape_meters_arr) > 0) 
        for shape_meters_arr in stop_times_wide.shape_meters
    ]
    
    # About 1/6 of the 6,000 shapes gets tagged as being False
    stop_times_wide = stop_times_wide.assign(
        is_monotonic = is_monotonic
    )
    
    inlining_shapes = stop_times_wide[
        stop_times_wide.is_monotonic == False].shape_array_key.unique()

    return inlining_shapes


def prep_stop_segments(analysis_date: str) -> dg.GeoDataFrame:

    stop_times_with_geom = stop_times_aggregated_to_shape_array_key(
        analysis_date
    ).repartition(npartitions=1)
    
    # Turn the stop_geometry and shape_geometry columns into geoseries
    shape_geoseries = gpd.GeoSeries(stop_times_with_geom.geometry.compute())
    stop_geoseries = gpd.GeoSeries(
        stop_times_with_geom.stop_geometry.compute())
    
    # Get projected shape_meters as dask array
    shape_meters_geoseries = wrangle_shapes.project_point_geom_onto_linestring(
        shape_geoseries,
        stop_geoseries,
        get_dask_array=True
    )
    
    # Attach dask array as a column
    stop_times_with_geom["shape_meters"] = shape_meters_geoseries
    
    stop_times_with_geom = stop_times_with_geom.repartition(npartitions=10)
    
    # Get the arrays of shape_array_keys to flag
    loopy_shapes = tag_shapes_with_stops_visited_twice(stop_times_with_geom)
    inlining_shapes = tag_shapes_with_inlining(stop_times_with_geom)
    
    # Create column where it's 1 if it needs super_project, 
    # 0 for normal shapely.project   
    stop_times_with_geom = stop_times_with_geom.assign(
        loop_or_inlining = stop_times_with_geom.apply(
            lambda x: 
            1 if x.shape_array_key in np.union1d(loopy_shapes, inlining_shapes)
            else 0, axis=1, 
            meta = ('loop_or_inlining', 'int8'))
    )
    
    return stop_times_with_geom


if __name__=="__main__":

    LOG_FILE = "../logs/prep_stop_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    stops_by_shape = prep_stop_segments(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"Prep stop segment df: {time1-start}")
    
    # Export this as partitioned parquet
    stops_by_shape.to_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}", overwrite=True
    )
   
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    