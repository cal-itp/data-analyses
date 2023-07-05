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
from segment_speed_utils.project_vars import SEGMENT_GCS, COMPILED_CACHED_VIEWS, PROJECT_CRS
from shared_utils import utils

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
    

def operators_with_data(
    gcs_folder: str, 
    file_name_prefix: str, 
    analysis_date: str
) -> list:
    """
    Return a list of operators with RT files on that day by looking for a
    certain file_name_prefix
    """
    all_files_in_folder = fs.ls(gcs_folder)
    
    files = [i for i in all_files_in_folder if f"{file_name_prefix}" in i]
    
    rt_operators = [i.split(f'{file_name_prefix}')[1]
                    .split(f'_{analysis_date}')[0] 
                    for i in files if analysis_date in i]
    
    return rt_operators


def find_day_after(analysis_date: str) -> str:
    """
    RT is UTC, so let's also download the day after.
    That way, when we localize to Pacific time, we don't lose
    info after 4pm Pacific (which is midnight UTC).
    """
    one_day_later = (pd.to_datetime(analysis_date).date() + 
                     datetime.timedelta(days=1)
                    )
    
    return one_day_later.isoformat()
        

def import_vehicle_positions(
    gcs_folder: str = SEGMENT_GCS, 
    file_name: str = "",
    file_type: Literal["df", "gdf"] = "df", 
    filters: tuple = None,
    columns: list = None,
    partitioned: bool = False,
) -> Union[dd.DataFrame, dg.GeoDataFrame]:
    
    if partitioned:
        file_name_sanitized = f"{utils.sanitize_file_path(file_name)}"
    else:
        file_name_sanitized = f"{utils.sanitize_file_path(file_name)}.parquet"
    
    if file_type == "df":
        df = dd.read_parquet(
            f"{gcs_folder}{file_name_sanitized}", 
            filters = filters,
            columns = columns
        ).drop_duplicates().reset_index(drop=True)
    
    elif file_type == "gdf":
        df = dg.read_parquet(
            f"{gcs_folder}{file_name_sanitized}", 
            filters = filters,
            columns = columns
        ).drop_duplicates().reset_index(drop=True).to_crs(PROJECT_CRS)
    
    return df

    
def import_segments(
    gcs_folder: str = SEGMENT_GCS, 
    file_name: str = "",
    filters: tuple = None,
    columns: list = None,
    partitioned: bool = False
) -> dg.GeoDataFrame:
    
    if partitioned:
        file_name_sanitized = f"{utils.sanitize_file_path(file_name)}"
        
        df = dg.read_parquet(
            f"{gcs_folder}{file_name_sanitized}", 
            filters = filters,
            columns = columns
        )
    else:
        file_name_sanitized = f"{utils.sanitize_file_path(file_name)}.parquet"
        
        df = gpd.read_parquet(
            f"{gcs_folder}{file_name_sanitized}", 
            filters = filters,
            columns = columns
        )
    
    df = df.drop_duplicates().reset_index(drop=True).to_crs(PROJECT_CRS)
    
    return df


def import_scheduled_trips(
    analysis_date: str, 
    filters: tuple = None,
    columns: list = [
        "feed_key", "name", "trip_id", 
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
    
    if get_pandas:
        trips = pd.read_parquet(FILE, filters = filters, columns = columns)
    else:
        trips = dd.read_parquet(FILE, filters = filters, columns = columns)
    
    return trips.drop_duplicates().reset_index(drop=True)


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