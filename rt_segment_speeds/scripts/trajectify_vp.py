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


def transform_array_of_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Try to load an array of points and explode this with 
    downloaded df.
    """
    t0 = datetime.datetime.now()
    
    df = df.assign(
        service_date = pd.to_datetime(df.service_date),
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
    """
    Get x, y out so it can be used with movingpandas.TrajectoryCollection.
    Might be able to go back to using fct_vehicle_locations
    if this is a Python data model with BigFrames, since the bottleneck
    is in how many rows we can download 
    (1 single day of vp has to be broken into 4+ batches to be put back as gdf)
    """
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
    
    See if we can grab just the underlying df from this and export.
    
    We should keep vp_key if we want to be able to keep the vp's location and 
    join it back to fct_vehicle_locations.
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
    #tc.add_distance(overwrite=True, name="distance_miles", units="mi")
    
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
    
    
    
    result_df = pd.concat(
        [traj.df.drop(columns="geometry").pipe(round_columns) for traj in tc], 
        axis=0, 
        ignore_index=True
    )
    
    end = datetime.datetime.now()
    logger.info(f"save out df: {end - start}")
    
    return result_df


def round_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Round to 3 decimal places
    """
    df = df.assign(
        distance_meters = df.distance_meters.round(3),
        speed_mph = df.speed_mph.round(3),
        acceleration_mph_per_sec = df.acceleration_mph_per_sec.round(3),
        angular_difference = df.angular_difference.round(3),
        direction = df.direction.round(3),
    )
    
    return df
                    

def export_results(
    trajectory_collection: mpd.TrajectoryCollection,
    export_file_name: str
):
    """
    Return results as line? Save out line segments gdf so we can plot easily, 
    but we might have to change this if it's not suitable to import 
    several days back in and aggregate.
    
    We might go with exporting point_gdf instead of line, unless we can figure out
    how to export just the .df.
    Having columns that can join back to vp_key in fct_vehicle_locations is best.
    
    https://github.com/movingpandas/movingpandas/blob/main/movingpandas/trajectory_collection.py#L150-L159
    The exporting is taking each trajectory and exporting, then concatenating with pandas.
    """
    t0 = datetime.datetime.now()
    
    #results = trajectory_collection.to_line_gdf().reset_index()
    results = trajectory_collection.to_point_gdf().reset_index()
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
    
    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()
        
        operators = pd.read_parquet(
            f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
            columns = ["gtfs_dataset_name"]
        ).gtfs_dataset_name.unique()
        
        
        df = pd.read_parquet(
                f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
                columns = [
                    "service_date", "gtfs_dataset_name",
                    "gtfs_dataset_key",
                    "trip_instance_key", 
                    "location_timestamp_pacific", 
                    "pt_array"
            ]
        ).pipe(
            transform_array_of_strings
        )
        
        for one_operator_name in sorted(operators):
            
            start_operator = datetime.datetime.now()
            print(f"start {one_operator_name}")
            cleaned_name = one_operator_name.replace(' ', '_').lower()
            
            df_subset = df[df.gtfs_dataset_name == one_operator_name].reset_index(drop=True)
            
            results = df_subset.pipe(
                explode_tuples
            ).pipe(
                add_movingpandas_deltas
            )
            
            results.to_parquet(
                f"{SEGMENT_GCS}"
                f"{EXPORT_FILE}_concat_{analysis_date}/{cleaned_name}.parquet",
                engine="pyarrow"
            )
            
            results_arrays = (
                results
                .groupby(["service_date", "gtfs_dataset_name", "trip_instance_key"])
                .agg({
                    "distance_meters": lambda x: list(x),
                    #"distance_miles": lambda x: list(x),
                    "timedelta": lambda x: list(x),
                    "speed_mph": lambda x: list(x),
                    "acceleration_mph_per_sec": lambda x: list(x),
                    "angular_difference": lambda x: list(x),
                    "direction": lambda x: list(x),
                })
                .reset_index()
            )
            
            results_arrays.to_parquet(
                f"{SEGMENT_GCS}"
                f"{EXPORT_FILE}_flat_{analysis_date}/{cleaned_name}.parquet",
                engine="pyarrow"
            )
            
            del results, results_arrays
            
            end_operator = datetime.datetime.now()
            logger.info(f"finished {one_operator_name}: {end_operator - start_operator}")

        
        end = datetime.datetime.now()
        logger.info(f"moving pandas: {analysis_date}: {end - start}")