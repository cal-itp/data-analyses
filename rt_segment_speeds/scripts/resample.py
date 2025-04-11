"""
Project vp point geometry onto shape.
Condense vp into linestring.
Resample every 5 seconds.
"""
import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
import sys

from loguru import logger
from numba import jit
from calitp_data_analysis import utils

from segment_speed_utils import helpers, vp_transform
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import rt_dates, geo_utils

subset_trips = [
    '00000ec4ec4f4cee317f06c981d4965f',
    '0000c1d4e6dddb9d472eba4dd5bff75e',
    '0001245feefb9de38772db6d349324c9',
    '000151b853273d961473d771438c943f',
    '00018c6b262a45ef8ce0e8126d96c96e'
]

def determine_batches(analysis_date: str) -> dict:
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}/",
        columns = ["gtfs_dataset_name"],
    ).drop_duplicates()
    
    large_operators = [
        "LA Metro Bus",
        "LA Metro Rail",
        "Bay Area 511"
    ]
  
    # If any of the large operator name substring is 
    # found in our list of names, grab those
    # be flexible bc "Vehicle Positions" and "VehiclePositions" present
    matching1 = [i for i in vp.gtfs_dataset_name 
                if any(name in i for name in large_operators)]
    
    remaining = [i for i in vp.gtfs_dataset_name if 
                 i not in matching1]
    
    # Batch large operators together and run remaining in 2nd query
    batch_dict = {}
    
    batch_dict[0] = matching1
    batch_dict[1] = remaining
    
    return batch_dict


def get_trips_by_batches(operator_list: list) -> list:
    
    subset_trips = pd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}/",
        filters = [["gtfs_dataset_name", "in", operator_list]],
        columns = ["trip_instance_key"],
    ).trip_instance_key.unique().tolist()
    
    return subset_trips
    

def create_vp_with_shape_and_project(analysis_date: str) -> gpd.GeoDataFrame:
    """
    """
    VP_USABLE = GTFS_DATA_DICT.speeds_tables.usable_vp 
    
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}/",
        columns = ["trip_instance_key", "location_timestamp_local", "x", "y"],

    ).pipe(
        geo_utils.vp_as_gdf, 
        crs = PROJECT_CRS
    )
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_id", "trip_instance_key", "shape_array_key"],
        get_pandas = True,

    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        get_pandas = True,
        crs = PROJECT_CRS,
        filters = [[("shape_array_key", "in", trips.shape_array_key.tolist())]]
    ).merge(
        trips,
        on = "shape_array_key",
        how = "inner"
    )
    
    vp_gdf = pd.merge(
        vp.rename(columns = {"geometry": "vp_geometry"}),
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "trip_instance_key",
        how = "inner"
    ).set_geometry("vp_geometry")
    
    vp_gdf = project_point_onto_shape(
        vp_gdf, 
        "shape_geometry", 
        "vp_geometry"
    ).rename(columns = {"projected_meters": "vp_meters"})
    
    return vp_gdf


def project_point_onto_shape(
    gdf: gpd.GeoDataFrame, 
    line_geom: str, 
    point_geom: str,
    crs: str = PROJECT_CRS
) -> gpd.GeoDataFrame:
    """
    """
    gdf = gdf.assign(
        projected_meters = gdf[line_geom].to_crs(crs).project(
            gdf[point_geom].to_crs(crs)
        )
    )
    
    return gdf


@jit(nopython=True)
def resample_timestamps(
    timestamp_arr: np.ndarray, 
    seconds_step: int
) -> np.ndarray:
    """
    resample timestamp array by every X seconds
    """
    return np.arange(min(timestamp_arr), max(timestamp_arr), step=seconds_step)


@jit(nopython=True)
def interpolate_distances_for_resampled_timestamps(
    resampled_timestamp_arr: np.ndarray, 
    original_timestamp_arr: np.ndarray, 
    projected_meters_arr: np.ndarray
) -> np.ndarray:
    """
    Interpolate and get (modeled/interpolated) Y's against X's.
    We have original X array, resampled X with more observations,
    and now we want to fill in those Y's in between with 
    numpy linear interpolation.
    """
    return np.interp(
        resampled_timestamp_arr, 
        original_timestamp_arr,
        projected_meters_arr
    )

def get_resampled_vp_points(
    gdf: gpd.GeoDataFrame,
    resampling_seconds_interval: int = 5
) -> gpd.GeoDataFrame:
    """
    Resample vp timestamps every 5 seconds to start,
    then interpolate the distances and fill in between.
    Return a gdf with 
    - resampled timestamps (array of timestamps coerced to seconds, then floats)
    - interpolated distances (array, meters along shape geometry)
    - geometry (convert distances to interpolated points along vp linestring
    (existing vp points condensed to linestring)
    """
    # Get timestamps that are datetime, change to display up to seconds, then convert to floats
    # This will be easier to use when we're visually inspecting
    timestamps_series = gdf.apply(
        lambda x: 
        x.location_timestamp_local.astype("datetime64[s]").astype("float64"), 
        axis=1
    )
    
    vp_meters_series = gdf.vp_meters
    vp_line_geometry = gdf.vp_geometry
    
    resampled_timestamps = [
        resample_timestamps(t, resampling_seconds_interval) 
        for t in timestamps_series
    ]
    
    interpolated_vp_meters = [
        interpolate_distances_for_resampled_timestamps(
            new_timestamps, orig_timestamps, vp_meters) 
            for new_timestamps, orig_timestamps, vp_meters 
            in zip(resampled_timestamps, timestamps_series, vp_meters_series)
    ]
    '''
    new_vp_points = [
        shapely.LineString(
            [vp_path.interpolate(one_vp_point) 
             for one_vp_point in vp_meters_arr]
        ) for vp_meters_arr, vp_path 
        in zip(interpolated_vp_meters, vp_line_geometry)
    ]    
    '''
    # Instead of coercing it to be a linestring, keep as array of points
    # that can be exploded
    new_vp_points = [
            [vp_path.interpolate(one_vp_point).coords[0]
             for one_vp_point in vp_meters_arr]
         for vp_meters_arr, vp_path 
        in zip(interpolated_vp_meters, vp_line_geometry)
    ]
    
    new_vp_x = [
        [one_point[0] for one_point in individual_trip] 
        for individual_trip in new_vp_points
    ]
    
    new_vp_y = [
        [one_point[1] for one_point in individual_trip] 
        for individual_trip in new_vp_points
    ]
     
    gdf2 = gdf.assign(
        resampled_timestamps = resampled_timestamps,
        interpolated_distances = interpolated_vp_meters,
        interpolated_vp_x = new_vp_x, 
        interpolated_vp_y = new_vp_y
        #new_vp_coords = new_vp_points,
        #geometry = gpd.GeoSeries(new_vp_points, crs = PROJECT_CRS)
    ).drop(
        columns = [
            "vp_geometry"
    ])
    
    modeled_cols = [
        "resampled_timestamps", 
        "interpolated_distances", 
        "interpolated_vp_x", 
        "interpolated_vp_y"
    ]
    
    gdf3 = gdf2[["trip_instance_key"] + modeled_cols]
    
    return gdf3


if __name__ == "__main__":
    
    LOG_FILE = "../logs/resampling.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    start = datetime.datetime.now()
    
    analysis_date = rt_dates.DATES["oct2024"]    
    
    gdf = create_vp_with_shape_and_project(
        analysis_date
    ).pipe(
        vp_transform.condense_point_geom_to_line,
        group_cols = ["trip_instance_key"],
        geom_col = "vp_geometry",
        array_cols = ["vp_meters", "location_timestamp_local"],
        sort_cols = ["trip_instance_key", "location_timestamp_local"]
    ).set_geometry("vp_geometry").set_crs(PROJECT_CRS)
    
    utils.geoparquet_gcs_export(
        gdf,
        SEGMENT_GCS,
        f"vp_condensed/vp_projected_{analysis_date}"
    )
    
    del gdf
    
    time1 = datetime.datetime.now()
    logger.info(f"project vp against shape and condense: {time1 - start}")
    
    
    batch_dict = determine_batches(analysis_date)
    
    for batch_number, subset_operators in batch_dict.items():
        
        subset_trips = get_trips_by_batches(subset_operators)    
   
        gdf = gpd.read_parquet(
            f"{SEGMENT_GCS}vp_condensed/vp_projected_{analysis_date}.parquet",
            filters = [[("trip_instance_key", "in", subset_trips)]]
        )
    
        results = get_resampled_vp_points(gdf)

        results.to_parquet(
            f"{SEGMENT_GCS}vp_condensed/vp_resampled_batch{batch_number}_{analysis_date}.parquet",
        )
        
        del results

    
    end = datetime.datetime.now()
    logger.info(f"resample and interpolate: {end - time1}")
    logger.info(f"{analysis_date}: project, condense, resample, interpolate: {end - start}")