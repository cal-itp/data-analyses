"""
Calculate a trip-level metric of spatial accuracy.

Newmark's GTFS RT spatial accuracy metric is simply
how many vehicle positions correctly join onto a buffered
shape.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from segment_speed_utils.project_vars import (COMPILED_CACHED_VIEWS, SEGMENT_GCS,
                                              analysis_date, PROJECT_CRS
                                             )

def grab_shape_keys_in_vp(analysis_date: str) -> pd.DataFrame:
    """
    Subset raw vp and find unique trip_instance_keys.
    Create crosswalk to link trip_instance_key to shape_array_key.
    """
    vp_trip_df = (pd.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet",
        columns=["gtfs_dataset_key", "trip_instance_key"])
        .drop_duplicates()
    )
    
    trips_with_shape = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet",
        columns=["trip_instance_key", "shape_array_key"],
    ).merge(
        vp_trip_df,
        on = "trip_instance_key",
        how = "inner"
    ).drop_duplicates().reset_index(drop=True)

    return trips_with_shape


def buffer_shapes(
    analysis_date: str,
    buffer_meters: int = 35,
    **kwargs
) -> gpd.GeoDataFrame:
    """
    Filter scheduled shapes down to the shapes that appear in vp.
    Buffer these.
    """
    shapes = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet",
        columns = ["shape_array_key", "geometry"],
        **kwargs
    ).to_crs(PROJECT_CRS)
    
    # to_crs takes awhile, so do a filtering on only shapes we need
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(buffer_meters)
    )
    
    return shapes


def attach_shape_geometry(
    analysis_date: str,
    trips_with_shape_subset: pd.DataFrame,
    buffer_meters: int = 35,
) -> gpd.GeoDataFrame:
    """
    Attach the shape geometry for a subset of shapes or trips.
    """
    shapes_subset = trips_with_shape_subset.shape_array_key.unique().tolist()
    
    shapes = buffer_shapes(
        analysis_date, 
        buffer_meters = 35, 
        filters = [[("shape_array_key", "in", shapes_subset)]]
    )

    trips_with_shape_geom = pd.merge(
        shapes,
        trips_with_shape_subset,
        on = "shape_array_key",
        how = "inner"
    )
    
    return trips_with_shape_geom


def merge_vp_with_shape_and_count(
    analysis_date: str,
    trips_with_shape_geom: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Merge vp with crosswalk and buffered shapes.
    Get vp count totals and vp within shape.
    """ 
    subset_rt_keys = trips_with_shape_geom.gtfs_dataset_key.unique().tolist()
    subset_trips = trips_with_shape_geom.trip_instance_key.unique().tolist()
    
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet",
        columns=["trip_instance_key", "location_timestamp_local", 
                 "geometry"],
        filters = [[("gtfs_dataset_key", "in", subset_rt_keys),
                    ("trip_instance_key", "in", subset_trips)]]
    ).to_crs(PROJECT_CRS)
    
    vp2 = pd.merge(
        vp,
        trips_with_shape_geom,
        on = "trip_instance_key",
        how = "inner"
    ).reset_index(drop=True)
    
    total_vp = total_vp_counts_by_trip(vp2)
    
    vp2 = vp2.assign(
        is_within = vp2.geometry_x.within(vp2.geometry_y)
    ).query('is_within==True')
    
    vps_in_shape = (vp2.groupby("trip_instance_key", 
                                observed = True, group_keys = False)
                    .agg({"location_timestamp_local": "count"})
                    .reset_index()
                    .rename(columns = {"location_timestamp_local": "vp_in_shape"})
                   )
        
    count_df = pd.merge(
        total_vp,
        vps_in_shape,
        on = "trip_instance_key",
        how = "left"
    )
    
    return count_df


def total_vp_counts_by_trip(vp: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Get a count of vp for each trip, whether or not those fall 
    within buffered shape or not
    """
    count_vp = (
        vp.groupby("trip_instance_key", 
                   observed=True, group_keys=False)
        .agg({"location_timestamp_local": "count"})
        .reset_index()
        .rename(columns={"location_timestamp_local": "total_vp"})
    )
    
    return count_vp

    
if __name__=="__main__":    
    
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    LOG_FILE = "../logs/spatial_accuracy.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
        
    start = datetime.datetime.now()

    trips_with_shape = grab_shape_keys_in_vp(analysis_date)

    shapes_in_vp = trips_with_shape.shape_array_key.unique().tolist()
    rt_operators = trips_with_shape.gtfs_dataset_key.unique().tolist()
    
    # Set up delayed lists of inputs by operator
    operator_trips_with_shapes = [
        delayed(attach_shape_geometry)(
            analysis_date,
            trips_with_shape[trips_with_shape.gtfs_dataset_key == rt_key],
            buffer_meters = 35,
        ) for rt_key in rt_operators 
    ]
        
    operator_results = [
        delayed(merge_vp_with_shape_and_count)(
            analysis_date, 
            operator_df
        ) for operator_df in operator_trips_with_shapes
    ]

    time1 = datetime.datetime.now()
    logger.info(f"get delayed by operator: {time1 - start}")
    
    results = [compute(i)[0] for i in operator_results]
    
    time2 = datetime.datetime.now()
    logger.info(f"compute results: {time2 - time1}")
    
    results_df = pd.concat(
        results, axis=0
    ).reset_index(drop=True)
        
    results_df.to_parquet(
        f"{SEGMENT_GCS}trip_summary/"
        f"vp_spatial_accuracy_{analysis_date}.parquet")
    
    end = datetime.datetime.now()
    logger.info(f"export: {end - time2}")
    logger.info(f"execution time: {end - start}")
    
    #client.close()