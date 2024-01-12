"""
Utility functions related to wrangling vehicle positions 
and segments.

Use this to see what shows up repeatedly 
and can be modularized in the future.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import gcsfs
import geopandas as gpd
import pandas as pd
import yaml

from typing import Literal, Union
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              COMPILED_CACHED_VIEWS,
                                              RT_SCHED_GCS,
                                              PROJECT_CRS)
from calitp_data_analysis import utils

fs = gcsfs.GCSFileSystem()

def get_parameters(
    config_file: str, 
    segment_type: Union["route_segments", "stop_segments"]
) -> dict:
    """
    Parse the config.yml file to get the parameters needed
    for working with route or stop segments.
    These parameters will be passed through the scripts when working 
    with vehicle position data.
    
    Returns a dictionary of parameters.
    """
    #https://aaltoscicomp.github.io/python-for-scicomp/scripts/
    with open(config_file) as f: 
        my_dict = yaml.safe_load(f)
        params_dict = my_dict[segment_type]
    
    return params_dict


def import_scheduled_trips(
    analysis_date: str, 
    filters: tuple = None,
    columns: list = [
        "gtfs_dataset_key", "name", "trip_id", 
        "shape_id", "shape_array_key", 
        "route_id", "route_key", "direction_id"
    ],
    get_pandas: bool = False
) -> dd.DataFrame:
    """
    Get scheduled trips info (all operators) for single day, 
    and keep subset of columns.
    """
    FILE = f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet"
    RENAME_DICT = {
        "gtfs_dataset_key": "schedule_gtfs_dataset_key"
    }
    
    if get_pandas:
        trips = pd.read_parquet(FILE, filters = filters, columns = columns)
    else:
        trips = dd.read_parquet(FILE, filters = filters, columns = columns)
    
    return (trips.drop_duplicates().reset_index(drop=True)
            .rename(columns = RENAME_DICT))


def import_scheduled_shapes(
    analysis_date: str, 
    filters: tuple = None,
    columns: list = ["shape_array_key", "geometry"],
    get_pandas: bool = False, 
    crs: str = PROJECT_CRS
) -> dg.GeoDataFrame: 
    """
    Import shapes.
    """
    FILE = f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet"
    
    if get_pandas: 
        shapes = gpd.read_parquet(FILE, filters = filters, 
                                  columns = columns)
    else:
        shapes = dg.read_parquet(FILE, filters = filters,
                                 columns = columns)
    
    # Don't do this expensive operation unless we have to
    if crs != shapes.crs:
        shapes = shapes.to_crs(crs)
        
    return shapes.drop_duplicates().reset_index(drop=True)


def import_scheduled_stop_times(
    analysis_date: str, 
    filters: tuple = None,
    columns: list = None
) -> dd.DataFrame:
    """
    Get scheduled stop times.
    """
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}st_{analysis_date}.parquet", 
        filters = filters,
        columns = columns
    )
    
    return stop_times.drop_duplicates().reset_index(drop=True)


def import_scheduled_stops(
    analysis_date: str,
    filters: tuple = None,
    columns: list = None,
    get_pandas: bool = False,
    crs: str = PROJECT_CRS
) -> dg.GeoDataFrame:
    """
    Get scheduled stops
    """
    FILE = f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet"
    
    if get_pandas:
        stops = gpd.read_parquet(FILE, filters = filters, 
                                 columns = columns)
    
    else:
        stops = dg.read_parquet(FILE, filters = filters,
                                columns = columns)
    
    if crs != stops.crs:
        stops = stops.to_crs(crs)
    
    return stops.drop_duplicates().reset_index(drop=True)


def import_assembled_stop_times_with_direction(
    analysis_date: str, 
    filters: tuple = None,
    columns: list = None,
    get_pandas: bool = False,
    crs: str = PROJECT_CRS
) -> dg.GeoDataFrame:
    """
    Get assembled stop times with direction 
    (which doesn't have all the stop_times
    columns, but does have trip_instance_key).
    """
    FILE = f"{RT_SCHED_GCS}stop_times_direction_{analysis_date}.parquet"
    
    if get_pandas:
        stop_times = gpd.read_parquet(FILE, filters = filters, 
                                      columns = columns)
    else:
        stop_times = dg.read_parquet(FILE,
            filters = filters, columns = columns
        )
    
    if crs != stop_times.crs:
        stop_times = stop_times.to_crs(crs)
    
    return stop_times.drop_duplicates().reset_index(drop=True)


def exclude_unusable_trips(
    vp_df: dd.DataFrame, 
    valid_trips: pd.DataFrame
) -> dd.DataFrame:
    """
    Supply a df of valid trips.
    Do an inner merge and pare down the vehicle positions df.
    `trip_id` may not be unique across operators, so 
    use `gtfs_dataset_key` and `trip_id`.
    """
    valid_vp_df = dd.merge(
        vp_df,
        valid_trips,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    ).reset_index(drop=True)
    
    return valid_vp_df


def remove_shapes_outside_ca(
    shapes: Union[gpd.GeoDataFrame, dg.GeoDataFrame]
) -> Union[gpd.GeoDataFrame, dg.GeoDataFrame]:
    """
    Remove shapes that are too far outside CA.
    We'll include border states with a gpd.sjoin(predicate='within')
    to make sure we get shapes that travel a tiny bit outside CA, but
    aren't ones like Amtrak.
    
    FlixBus is another like Amtrak, with far flung routes.
    """
    # Can't get relative path working within importable segment_speed_utils
    #us_states = catalog.us_states.read()
    # https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/shared_data_catalog.yml
    us_states = gpd.read_file(
        "https://services.arcgis.com/ue9rwulIoeLEI9bj/"
        "arcgis/rest/services/US_StateBoundaries/FeatureServer/0/"
        "query?outFields=*&where=1%3D1&f=geojson"
    )
    
    border_states = ["CA", "NV", "AZ", "OR"]
    
    SHAPE_CRS = shapes.crs.to_epsg()

    # Filter to California
    ca = us_states.query(
        'STATE_ABBR in @border_states'
    ).dissolve()[["geometry"]].to_crs(SHAPE_CRS)
    
    
    # Be aggressive and keep if shape
    # is within (does not cross CA + border state boundaries)
    if isinstance(shapes, dg.GeoDataFrame):
        shapes_within_ca = dg.sjoin(
            shapes,
            ca,
            how = "inner",
            predicate = "within"
        ).drop(columns = "index_right")
    
    else:
        shapes_within_ca = gpd.sjoin(
            shapes,
            ca,
            how = "inner",
            predicate = "within",
        ).drop(columns = "index_right")
    
    return shapes_within_ca


def if_exists_then_delete(filepath: str):
    """
    Check if file exists in GCS and delete.
    For partitioned parquets, which are saved as folders, we need
    to use recursive=True.
    """
    if fs.exists(filepath):
        if fs.isdir(filepath):
            fs.rm(filepath, recursive=True)
        else:
            fs.rm(filepath)
    
    return