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
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS,
                                              COMPILED_CACHED_VIEWS,
                                              RT_SCHED_GCS,
                                              PROJECT_CRS
                                             )

def grab_shape_keys_in_vp(analysis_date: str) -> pd.DataFrame:
    """
    Subset raw vp and find unique trip_instance_keys.
    Create crosswalk to link trip_instance_key to shape_array_key.
    """
    vp_trip_df = (pd.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet",
        columns=["trip_instance_key"])
        .drop_duplicates()
        .dropna(subset="trip_instance_key")
    )
    
    # Make sure we have a shape geometry too
    # otherwise map_partitions will throw error
    shapes = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet",
        columns = ["shape_array_key"],
    ).dropna().drop_duplicates()
    
    trips_with_shape = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        get_pandas = True
    ).merge(
        shapes,
        on = "shape_array_key",
        how = "inner"
    ).merge(
        vp_trip_df,
        on = "trip_instance_key",
        how = "inner"
    ).drop_duplicates().dropna().reset_index(drop=True)

    return trips_with_shape


def buffer_shapes(
    analysis_date: str,
    trips_with_shape_subset: pd.DataFrame,
    buffer_meters: int = 35,
    **kwargs
) -> gpd.GeoDataFrame:
    """
    Filter scheduled shapes down to the shapes that appear in vp.
    Buffer these.
    
    Attach the shape geometry for a subset of shapes or trips.
    """
    shapes_subset = trips_with_shape_subset.shape_array_key.unique().tolist()
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        filters = [[("shape_array_key", "in", shapes_subset)]],
        crs = PROJECT_CRS,
        get_pandas = True
    )
    
    # to_crs takes awhile, so do a filtering on only shapes we need
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(buffer_meters)
    )
    
    trips_with_shape_geom = pd.merge(
        shapes,
        trips_with_shape_subset,
        on = "shape_array_key",
        how = "inner"
    )
    
    return trips_with_shape_geom


def merge_vp_with_shape_and_count(
    vp: dd.DataFrame,
    trips_with_shape_geom: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Merge vp with crosswalk and buffered shapes.
    Get vp count totals and vp within shape.
    """
    vp_gdf = gpd.GeoDataFrame(
        vp,
        geometry = gpd.points_from_xy(vp.x, vp.y),
        crs = PROJECT_CRS
    )
    
    vp2 = pd.merge(
        vp_gdf,
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
    
    count_df = count_df.assign(
        vp_in_shape = count_df.vp_in_shape.fillna(0).astype("int32"),
        total_vp = count_df.total_vp.fillna(0).astype("int32")
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
    from update_vars import analysis_date_list
    #from dask.distributed import LocalCluster, Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    #cluster = LocalCluster(n_workers = 2)
    
    LOG_FILE = "../logs/spatial_accuracy.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    for analysis_date in analysis_date_list:
    
        logger.info(f"Analysis date: {analysis_date}")

        start = datetime.datetime.now()

        # Create crosswalk of trip_instance_keys to shape_array_key
        trips_with_shape = grab_shape_keys_in_vp(analysis_date)

        trips_with_shape_geom = buffer_shapes(
            analysis_date,
            trips_with_shape,
            buffer_meters = 35
        )

        # Import vp and partition it
        vp = dg.read_parquet(
            f"{SEGMENT_GCS}vp_{analysis_date}.parquet",
        ).to_crs(PROJECT_CRS)
        
        vp = vp.assign(
            x = vp.geometry.x,
            y = vp.geometry.y
        ).drop(columns = "geometry")

        vp = vp.repartition(npartitions = 100).persist()

        results = vp.map_partitions(
            merge_vp_with_shape_and_count,
            trips_with_shape_geom,
            meta = {
                "trip_instance_key": "str",
                "total_vp": "int32",
                "vp_in_shape": "int32"
            }, 
            align_dataframes = False
        )

        time1 = datetime.datetime.now()
        logger.info(f"use map partitions to find within shape vp: {time1 - start}")

        results = results.compute()

        time2 = datetime.datetime.now()
        logger.info(f"compute results: {time2 - time1}")

        results.to_parquet(
            f"{RT_SCHED_GCS}vp_spatial_accuracy_{analysis_date}.parquet")

        end = datetime.datetime.now()
        logger.info(f"export: {end - time2}")
        logger.info(f"execution time: {end - start}")
    
    #client.close()
    #cluster.close()