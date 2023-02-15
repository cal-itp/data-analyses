"""
Utility functions related to wrangling vehicle positions 
and segments.

Use this to see what shows up repeatedly 
and can be modularized in the future.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import gcsfs

from typing import Literal, Union
from update_vars import SEGMENT_GCS, PROJECT_CRS

fs = gcsfs.GCSFileSystem()

def sanitize_parquet_filename(file_name: str): 
    return file_name.replace('.parquet', '')

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


def import_vehicle_positions(
    gcs_folder: str = SEGMENT_GCS, 
    file_name: str = "",
    file_type: Literal["df", "gdf"] = "df", 
    filters: tuple = None,
    columns: list = None,
    partitioned: bool = False,
) -> Union[dd.DataFrame, dg.GeoDataFrame]:
    
    if partitioned:
        file_name_sanitized = f"{sanitize_parquet_filename(file_name)}"
    else:
        file_name_sanitized = f"{sanitize_parquet_filename(file_name)}.parquet"
    
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
        file_name_sanitized = f"{sanitize_parquet_filename(file_name)}"
    else:
        file_name_sanitized = f"{sanitize_parquet_filename(file_name)}.parquet"
        
    df = dg.read_parquet(
        f"{gcs_folder}{file_name_sanitized}", 
        filters = filters,
        columns = columns
    ).drop_duplicates().reset_index(drop=True).to_crs(PROJECT_CRS)
    
    return df


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