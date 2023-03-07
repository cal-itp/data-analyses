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
    f = open(config_file)
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
    else:
        file_name_sanitized = f"{utils.sanitize_file_path(file_name)}.parquet"
        
    df = dg.read_parquet(
        f"{gcs_folder}{file_name_sanitized}", 
        filters = filters,
        columns = columns
    ).drop_duplicates().reset_index(drop=True).to_crs(PROJECT_CRS)
    
    return df


def import_scheduled_trips(
    analysis_date: str, 
    filters: tuple = None,
    columns: list = [
        "feed_key", "name", "trip_id", 
        "shape_id", "shape_array_key", 
        "route_id", "route_key", "direction_id"
    ]
) -> dd.DataFrame:
    """
    Get scheduled trips info (all operators) for single day, 
    and keep subset of columns.
    """
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet", 
        filters = filters,
        columns = columns
    )
    
    return trips


def import_scheduled_shapes(
    analysis_date: str, 
    filters: tuple = None,
    columns: list = ["shape_array_key", "geometry"]
) -> dg.GeoDataFrame: 
    """
    Import routelines and add route_length.
    """
    shapes = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet",
        filters = filters,
        columns = columns
    ).to_crs(PROJECT_CRS)
    
    return shapes


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
    
    return stop_times


def import_scheduled_stops(
    analysis_date: str,
    filters: tuple = None,
    columns: list = None
) -> dg.GeoDataFrame:
    """
    Get scheduled stops
    """
    stops = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet",
        filters = filters,
        columns = columns
    ).to_crs(PROJECT_CRS)
    
    return stops


def sjoin_vehicle_positions_to_segments(
    vehicle_positions: dg.GeoDataFrame, 
    segments: dg.GeoDataFrame,
    route_tuple: tuple,
    segment_identifier_cols: list
) -> dd.DataFrame:
    """
    Spatial join vehicle positions for an operator
    to buffered route segments.
    
    Returns a dd.DataFrame. geometry seems to make the 
    compute extremely large. 
    """    
    route_col, route_value = route_tuple[0], route_tuple[1]
    
    vehicle_positions_subset = (vehicle_positions[
        vehicle_positions[route_col]==route_value]
        .reset_index(drop=True))
        
    segments_subset = (segments[
        segments[route_col]==route_value]
       .reset_index(drop=True)
      )
    
    # Once we filter for the route_dir_identifier, don't need to include
    # it into segment_cols, otherwise, it'll show up as _x and _y
    vp_to_seg = dg.sjoin(
        vehicle_positions_subset,
        segments_subset[
            segment_identifier_cols + ["geometry"]],
        how = "inner",
        predicate = "within"
    ).drop(columns = "index_right").drop_duplicates().reset_index(drop=True)
    
    
    # Drop geometry and return a df...eventually,
    # can attach point geom back on, after enter/exit points are kept
    # geometry seems to be a big issue in the compute
    vp_to_seg2 = vp_to_seg.assign(
        lon = vp_to_seg.geometry.x,
        lat = vp_to_seg.geometry.y,
    )
    
    ddf = vp_to_seg2.drop(columns = ["geometry"])
    
    return ddf


def exclude_unusable_trips(vp_df: dd.DataFrame, 
                           valid_trip_ids: list) -> dd.DataFrame:
    """
    PLACEHOLDER FUNCTION
    Figure out trip-level diagnostics first.
    Supply a list of valid trips or trips to exclude?
    """
    return vp_df[vp_df.trip_id.isin(trips_list)].reset_index(drop=True)