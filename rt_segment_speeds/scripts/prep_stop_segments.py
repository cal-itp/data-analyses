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

from calitp_data_analysis import utils
from segment_speed_utils import (helpers, gtfs_schedule_wrangling,
                                 wrangle_shapes)
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS)


def trip_with_most_stops(analysis_date: str) -> pd.DataFrame:
    """
    Count the number of stop_id-stop_sequence rows for each trip.
    """
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "feed_key", "name", 
                   "trip_id", "trip_instance_key", 
                   "shape_array_key"],
        get_pandas = True
    )

    trips = gtfs_schedule_wrangling.exclude_scheduled_operators(
        trips, 
        exclude_me = ["Amtrak Schedule", "*Flex"]
    ).drop(columns = "name")
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "trip_id", "stop_id", "stop_sequence"]
    )
    
    stops_per_trip = (stop_times.groupby(["feed_key", "trip_id"], 
                                         observed=True, group_keys=False)
                      .agg({"stop_id": "count"})
                      .reset_index()
                      .rename(columns = {"stop_id": "n_stops"})
                     ).compute()
    
    df = pd.merge(
        trips,
        stops_per_trip,
        on = ["feed_key", "trip_id"],
        how = "inner"
    )
    
    df = df.assign(
          max_stops = (df.groupby("shape_array_key")
                       .n_stops
                       .transform("max").fillna(0).astype(int)
                      )
    ).query('max_stops == n_stops')
    
    # We can still have multiple trips within shape with same amt of stops
    # this is majority of cases
    df2 = (df.sort_values(["shape_array_key", "trip_id"], 
                          ascending=[True, True])
           .drop_duplicates(subset="shape_array_key")
           .reset_index(drop=True)
           .drop(columns = ["max_stops", "n_stops"])
          )
    
    return df2

    
def stop_times_aggregated_to_shape_array_key(
    analysis_date: str,
) -> gpd.GeoDataFrame:
    """
    For stop-to-stop segments, we need to aggregate stop_times,
    which comes at trip-level, to shape level. 
    From trips, attach shape_array_key, then merge to stop_times.
    Then attach stop's point geom.
    """
    
    trips_with_shape = trip_with_most_stops(analysis_date)[
        ["trip_instance_key", "shape_array_key"]]
    
    keep_trips = trips_with_shape.trip_instance_key.unique().tolist()
    
    stop_times = gpd.read_parquet(
        f"{RT_SCHED_GCS}stop_times_direction_{analysis_date}.parquet",
        filters = [[("trip_instance_key", "in", keep_trips)]]
    ).rename(columns = {
        "trip_instance_key": "st_trip_instance_key", 
        "geometry": "stop_geometry"
    })
    
    # Note: there can be duplicate shape_array_key because of multiple feeds
    # Drop them now so we keep 1 set of shape-stop info
    st_with_stop_geom = (stop_times.sort_values("schedule_gtfs_dataset_key")
                         .drop_duplicates(subset=[
                             "shape_array_key", "st_trip_instance_key", 
                             "stop_sequence"])
                         .reset_index(drop=True)
                        )

    return st_with_stop_geom


def tag_shapes_with_stops_visited_twice(
    stop_times: Union[pd.DataFrame, gpd.GeoDataFrame]
) -> np.ndarray:
    """
    Aggregate stop times by shape_array_key.
    For each stop_id, count how many stop_sequence values there are. 
    More than 1 means that the same stop_id is revisited on the same trip.
    
    Ex: a stop in a plaza that acts as origin and destination.
    """
    stop_visits = (stop_times.groupby(
                    ["shape_array_key", "stop_id"], 
                    observed=True, group_keys=False)
                  .agg({"stop_sequence": "count"}) 
                   #nunique doesn't work in dask
                  .reset_index()
                 )
    
    
    # If any of the trips within that shape revisits a stop, keep that shape
    loopy_shapes = (stop_visits[stop_visits.stop_sequence > 1]
                    .shape_array_key
                    .unique()
                 )
    
    return loopy_shapes


def tag_shapes_with_inlining(
    stop_times: Union[pd.DataFrame, gpd.GeoDataFrame]
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
                              "stop_sequence", "shape_meters"]].drop_duplicates()
        
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


def project_stop_to_shape(
    stop_times: dg.GeoDataFrame,
    shapes: dg.GeoDataFrame
):
    """
    Project the stop geometry against the shape geometry.
    Use map partitions, which treats each partition
    as pd.DataFrame or gpd.GeoDataFrame
    """
    gdf = pd.merge(
        stop_times,
        shapes,
        on = "shape_array_key",
        how = "inner"
    )
        
    # Get projected shape_meters 
    gdf["shape_meters"] = gdf.apply(
        lambda x: x.geometry.project(x.stop_geometry), 
        axis=1, 
    )
    
    df = gdf.drop(columns = "geometry")

    return df



def prep_stop_segments(analysis_date: str) -> dg.GeoDataFrame:

    stop_times_with_geom = stop_times_aggregated_to_shape_array_key(
        analysis_date
    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date, 
        columns = ["shape_array_key", "geometry"],
        get_pandas = False,
        crs = PROJECT_CRS
    ).dropna(subset=["shape_array_key", "geometry"])
  
    
    # Use map partitions to get shape_meters
    existing_dtypes = stop_times_with_geom.dtypes.to_dict()

    stop_times_with_geom2 = stop_times_with_geom.map_partitions(
        project_stop_to_shape,
        shapes,
        meta = {**existing_dtypes,
                "shape_meters": "float64"},
        align_dataframes = False,
    ).compute()
    
    
    # Get the arrays of shape_array_keys to flag
    # Always return np arrays, need to compute
    loopy_shapes = tag_shapes_with_stops_visited_twice(
        stop_times_with_geom2)
    
    inlining_shapes = tag_shapes_with_inlining(stop_times_with_geom2)
    
    # Create column where it's 1 if it needs super_project, 
    # 0 for normal shapely.project   
    stop_times_with_geom2 = stop_times_with_geom2.assign(
        loop_or_inlining = stop_times_with_geom2.apply(
            lambda x: 
            1 if x.shape_array_key in np.union1d(loopy_shapes, inlining_shapes)
            else 0, axis=1, 
        ).astype("int8")
    )
    
    return stop_times_with_geom2


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
        f"{SEGMENT_GCS}stops_projected_{analysis_date}_test.parquet", 
    )
   
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    