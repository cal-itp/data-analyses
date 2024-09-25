"""
Create the bus-related hqta_types

These are hqta_types: 
* major_stop_bus: the bus stop within the above intersection does not necessarily have
the highest trip count
* hq_corridor_bus: stops along the HQ transit corr (may not be highest trip count)

- <1 min in v2, but left the query in
- v1 in combine_and_visualize.ipynb
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

import _utils
import C1_prep_pairwise_intersections as prep_clip

from calitp_data_analysis import utils
from segment_speed_utils import helpers
from update_vars import (GCS_FILE_PATH, analysis_date, 
                         PROJECT_CRS, SEGMENT_BUFFER_METERS
                        )

def buffer_around_intersections(buffer_size: int) -> gpd.GeoDataFrame: 
    """
    Draw 50 m buffers around intersections to better catch stops
    that might fall within it.
    """
    gdf = gpd.read_parquet(
        f"{GCS_FILE_PATH}all_intersections.parquet"
    )
    
    gdf = gdf.assign(
        geometry = gdf.geometry.buffer(buffer_size)
    )

    return gdf 


def create_major_stop_bus(
    all_stops: gpd.GeoDataFrame, 
    bus_intersections: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Designate those hqta_type == major_stop_bus
    
    Only operators who have stops appearing in the clipped bus corridor intersections
    are eligible.
    Of these operators, find all their other stops that also show up in the clipped 
    intersections.
    """
    # Narrow down all stops to only include stops from operators
    # that also have some bus corridor intersection result
    included_operators = bus_intersections.schedule_gtfs_dataset_key.unique()
    major_stops = all_stops[
        all_stops.schedule_gtfs_dataset_key.isin(included_operators)
    ]
    
    major_bus_stops_in_intersections = (
        gpd.sjoin(
            major_stops,
            bus_intersections[["schedule_gtfs_dataset_key", "geometry"]],
            how = "inner",
            predicate = "within",
            lsuffix="primary", rsuffix="secondary"
        ).drop_duplicates(
            subset=[
                "schedule_gtfs_dataset_key_primary", "stop_id", 
                "schedule_gtfs_dataset_key_secondary"])
    ).reset_index(drop=True)
    
    stops_in_intersection = (
        major_bus_stops_in_intersections.assign(
            hqta_type = "major_stop_bus",
        )[["schedule_gtfs_dataset_key_primary", 
            "schedule_gtfs_dataset_key_secondary", 
            "stop_id", "geometry", "hqta_type"]]
    )
    
    return stops_in_intersection


def create_stops_along_corridors(all_stops: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Designate those hqta_type == hq_corridor_bus
    
    These are bus stops that lie within the HQ corridor, but 
    are not the stops that have the highest trip count.
    They may also be stops that don't meet the HQ corridor threshold, but
    are stops that physically reside in the corridor.
    """
    bus_corridors = (prep_clip.prep_bus_corridors(is_hq_corr = True)
                     [["hqta_segment_id", "geometry"]]
                    )
    
    stop_cols = ["schedule_gtfs_dataset_key", "stop_id"]
    
    stops_in_hq_corr = (
        gpd.sjoin(
            all_stops, 
            bus_corridors[["geometry"]],
            how = "inner", 
            predicate = "intersects"
        ).drop_duplicates(subset=stop_cols)
        .reset_index(drop=True)
    )
    
    stops_in_hq_corr2 = (
        stops_in_hq_corr.assign(
            hqta_type = "hq_corridor_bus",
        )[stop_cols + ["hqta_type", "geometry"]]
        .pipe(_utils.primary_rename)
    )
    
    return stops_in_hq_corr2


if __name__ == "__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    start = datetime.datetime.now()
    
    # Start with the gdf of all the hqta_segments
    # that have a sjoin with an orthogonal route
    bus_intersections = buffer_around_intersections(SEGMENT_BUFFER_METERS)

    # Grab point geom with all stops
    gtfs_keys = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "gtfs_dataset_key"],
        get_pandas=True
    )
    
    all_stops = helpers.import_scheduled_stops(
        analysis_date,
        get_pandas = True,
        columns = ["feed_key", "stop_id", "geometry"],
        crs = PROJECT_CRS
    ).merge(
        gtfs_keys,
        on = "feed_key",
    ).drop(columns = "feed_key")
        
    # Create hqta_type == major_stop_bus
    major_stop_bus = create_major_stop_bus(all_stops, bus_intersections)

    # Create hqta_type = hq_corridor_bus
    stops_in_hq_corr = create_stops_along_corridors(all_stops)
    
    # Export to GCS    
    utils.geoparquet_gcs_export(
        major_stop_bus, 
        GCS_FILE_PATH,
        "major_stop_bus"
    )
    
    utils.geoparquet_gcs_export(
        stops_in_hq_corr,
        GCS_FILE_PATH,
        "stops_in_hq_corr"
    )
    
    end = datetime.datetime.now()
    logger.info(
        f"C3_create_bus_hqta_types {analysis_date} "
        f"execution time: {end - start}"
    )
    
    #client.close()