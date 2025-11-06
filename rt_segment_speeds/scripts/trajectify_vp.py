"""
Using movingpandas and try to get as many of the 
columns we can calculated.
This will make it easier to work into as a Python data model
when we use BigFrames.
"""
import datetime
import geopandas as gpd
import google.auth
import pandas as pd
import movingpandas as mpd
import numpy as np
import shapely
import sys

from loguru import logger

from calitp_data_analysis import utils
from shared_utils import rt_dates
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT

credentials, project = google.auth.default()

def transform_linestring(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Try shapely.get_coordinates with downloaded gdf.
    """
    gdf = gdf.assign(
        pt_array = gdf.apply(lambda row: shapely.get_coordinates(row.geometry), axis=1),
    ).drop(columns = ["geometry"])

    return gdf

def transform_array_of_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Try to load an array of points and explode this with 
    downloaded df.
    """
    t0 = datetime.datetime.now()
    
    df = df.assign(
        pt_array = df.apply(
            lambda x: 
            np.array(
                shapely.get_coordinates(
                    [shapely.wkt.loads(p) for p in x.pt_array])
            ), 
            axis=1
        ),

    )
    
    t1 = datetime.datetime.now()
    logger.info(f"parse array of strings: {t1 - t0}")
    
    return df

def explode_tuples(gdf) -> pd.DataFrame:
    
    t0 = datetime.datetime.now()
    
    gdf = gdf.explode(
        column=[ "location_timestamp_pacific", "pt_array"]
    ).reset_index(drop=True)
        
    gdf = gdf.assign(
        x = gdf.apply(lambda x: x.pt_array[0], axis=1),
        y = gdf.apply(lambda x: x.pt_array[1], axis=1),  
    ).drop(columns = "pt_array")
    
    t1 = datetime.datetime.now()
    logger.info(f"explode array to long: {t1 - t0}")
    
    return gdf

def explode_array_by_trip(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get x, y out so it can be used with movingpandas.TrajectoryCollection.
    Might be able to go back to using fct_vehicle_locations
    if this is a Python data model with BigFrames, since the bottleneck
    is in how many rows we can download 
    (1 single day of vp has to be broken into 4+ batches to be put back as gdf)
    """
    t0 = datetime.datetime.now()

    # Try a version of this where pt_array has not been converted to linestring
    # and see if it works
    df = df.assign(
        x = df.apply(lambda row: np.array([shapely.wkt.loads(p).x for p in row.pt_array]), axis=1),
        y = df.apply(lambda row: np.array([shapely.wkt.loads(p).y for p in row.pt_array]), axis=1),
    ).drop(columns = ["pt_array"])
    
    df = df.explode(
        column=["location_timestamp_pacific", "x", "y"]
    ).reset_index(drop=True)
    
    t1 = datetime.datetime.now()
    logger.info(f"explode array to long: {t1 - t0}")

    return df


def explode_path_by_trip(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Get x, y out so it can be used with movingpandas.TrajectoryCollection.
    Might be able to go back to using fct_vehicle_locations
    if this is a Python data model with BigFrames, since the bottleneck
    is in how many rows we can download 
    (1 single day of vp has to be broken into 4+ batches to be put back as gdf)
    """
    t0 = datetime.datetime.now()

    # Try a version of this where pt_array has not been converted to linestring
    # and see if it works
    gdf = gdf.assign(
        x = gdf.apply(lambda x: np.array([p[0] for p in x.geometry.coords]), axis=1),
        y = gdf.apply(lambda x: np.array([p[1] for p in x.geometry.coords]), axis=1),
    ).drop(columns = "geometry")

    df = gdf.explode(
        column=["location_timestamp_pacific", "x", "y"]
    ).reset_index(drop=True)
    
    t1 = datetime.datetime.now()
    logger.info(f"explode path to long: {t1 - t0}")

    return df


def add_movingpandas_deltas(
    df: pd.DataFrame, 
    traj_id_col: str = "trip_instance_key",
    obj_id_col: str = None, #vp_key
    x: str = "x",
    y: str = "y", 
    t: str = "location_timestamp_pacific"
) -> mpd.TrajectoryCollection:
    """
    Take df and make into movingpandas.TrajectoryCollection, then
    add all the columns we can based on our raw vp.
    Decide how to handle it after.
    """
    start = datetime.datetime.now()

    tc = mpd.TrajectoryCollection(
        df, 
        traj_id_col = traj_id_col, 
        obj_id_col = obj_id_col, 
        x=x, y=y, t=t
    )
        
    # Add all the columns we can add
    tc.add_distance(overwrite=True, name="distance_meters", units="m")
    tc.add_distance(overwrite=True, name="distance_miles", units="mi")
    
    t1 = datetime.datetime.now()
    logger.info(f"add distance: {t1 - start}")
    
    tc.add_timedelta(overwrite=True)
    
    t2 = datetime.datetime.now()
    logger.info(f"add timedelta: {t2 - t1}")
    
    tc.add_speed(overwrite=True, name="speed_mph", units=("mi", "h"))
    tc.add_acceleration(
        overwrite=True, name="acceleration_mph_per_sec", units=("mi", "h", "s")
    )

    t3 = datetime.datetime.now()
    logger.info(f"add speed/acceleration: {t3 - t2}")
    
    tc.add_angular_difference(overwrite=True)
    tc.add_direction(overwrite=True)

    t4 = datetime.datetime.now()
    logger.info(f"add angular difference/direction: {t4 - t3}")
    
    end = datetime.datetime.now()
    logger.info(f"add movingpandas columns: {end - start}")

    return tc


def export_results(
    trajectory_collection: mpd.TrajectoryCollection,
    export_file_name: str
):
    """
    Return results as line? Save out line segments gdf so we can plot easily, 
    but we might have to change this if it's not suitable to import 
    several days back in and aggregate.
    """
    t0 = datetime.datetime.now()
    
    results = trajectory_collection.to_line_gdf().reset_index()
    #results = trajectory_collection.to_point_gdf().reset_index()
    #results = trajectory_collection.df
    
    utils.geoparquet_gcs_export(
        results,
        f"{SEGMENT_GCS}",
        export_file_name
    )
    
    t1 = datetime.datetime.now()
    logger.info(f"export: {t1 - t0}")
    
    return
    
    
if __name__ == "__main__":

    LOG_FILE = "../logs/movingpandas_pipeline.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    

    INPUT_FILE = GTFS_DATA_DICT.speeds_tables.vp_path
    EXPORT_FILE = GTFS_DATA_DICT.speeds_tables.vp_trajectory
    
    analysis_date_list = [
        rt_dates.DATES[f"{m}2025"] for m in ["sep"] 
    ]
    
    # see how long LA Metro Bus takes...
    # may have to split out some of the larger ones to do separately
    # doing it all operators in 1 day seems to get hung up
    batch0 = [
        "LA Metro Bus Vehicle Positions"
    ]
    
    
    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()
        
        gdf = pd.read_parquet(
            f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
            #storage_options={"token": credentials.token},
            filters = [[("gtfs_dataset_name", "in", batch0)]],
            columns = [
                "service_date", 
                "trip_instance_key", 
                "location_timestamp_pacific", 
                "pt_array"
            ]
        ).pipe(transform_array_of_strings).pipe(explode_tuples)

        traj_results = add_movingpandas_deltas(gdf)
        export_results(traj_results, f"mpd_batch/batch0/{EXPORT_FILE}_{analysis_date}")
        
        end = datetime.datetime.now()
        logger.info(f"moving pandas step: {analysis_date}: {end - start}")