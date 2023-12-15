"""
Create the bus-related hqta_types

These are hqta_types: 
* major_stop_bus: the bus stop within the above intersection does not necessarily have
the highest trip count
* hq_corridor_bus: stops along the HQ transit corr (may not be highest trip count)

- <1 min in v2, but left the query in
- v1 in combine_and_visualize.ipynb
"""
import datetime as dt
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

import C1_prep_pairwise_intersections as prep_clip
from calitp_data_analysis import utils
from utilities import catalog_filepath, GCS_FILE_PATH
from update_vars import analysis_date, PROJECT_CRS
from segment_speed_utils import helpers


# Input files
ALL_INTERSECTIONS = catalog_filepath("all_intersections")

def buffer_around_intersections(buffer_size: int = 50) -> gpd.GeoDataFrame: 
    """
    Draw 50 m buffers around intersections to better catch stops
    that might fall within it.
    """
    gdf = gpd.read_parquet(ALL_INTERSECTIONS)
    
    gdf = gdf.assign(
        geometry = gdf.geometry.buffer(buffer_size)
    )

    return gdf 


def create_major_stop_bus(all_stops: gpd.GeoDataFrame, 
                          bus_intersections: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Designate those hqta_type == major_stop_bus
    
    Only operators who have stops appearing in the clipped bus corridor intersections
    are eligible.
    Of these operators, find all their other stops that also show up in the clipped 
    intersections.
    """
    # Narrow down all stops to only include stops from operators
    # that also have some bus corridor intersection result
    included_operators = bus_intersections.feed_key.unique()
    major_stops = all_stops[all_stops.feed_key.isin(included_operators)]
    
    major_bus_stops_in_intersections = (
        gpd.sjoin(
            major_stops,
            bus_intersections[["feed_key", "geometry"]],
            how = "inner",
            predicate = "within"
        ).drop(columns = "index_right")
        .drop_duplicates(
            subset=["feed_key_left", "stop_id", "feed_key_right"])
    ).reset_index(drop=True)
    
    stops_in_intersection = (
        major_bus_stops_in_intersections.assign(
            hqta_type = "major_stop_bus",
            ).rename(columns = 
                     {"feed_key_left": "feed_key_primary", 
                      "feed_key_right": "feed_key_secondary",
                     })
          [["feed_key_primary", "feed_key_secondary", 
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
    bus_corridors = (prep_clip.prep_bus_corridors()
                     [["hqta_segment_id", "geometry"]].compute()
                    )
    
    stop_cols = ["feed_key", "stop_id"]
    
    stops_in_hq_corr = (gpd.sjoin(
                            all_stops, 
                            bus_corridors[["geometry"]],
                            how = "inner", 
                            predicate = "intersects"
                        ).drop(columns = "index_right")
                        .drop_duplicates(subset=stop_cols)
                        .reset_index(drop=True)
                       )
    
    stops_in_hq_corr2 = (stops_in_hq_corr.assign(
                            hqta_type = "hq_corridor_bus",
                        )[stop_cols + ["hqta_type", "geometry"]]
                         .rename(columns = {"feed_key": "feed_key_primary"})
                        )
    
    return stops_in_hq_corr2


if __name__ == "__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    logger.add("./logs/C3_create_bus_hqta_types.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
    # Start with the gdf of all the hqta_segments
    # that have a sjoin with an orthogonal route
    bus_intersections = buffer_around_intersections(buffer_size=100)

    # Grab point geom with all stops
    all_stops = helpers.import_scheduled_stops(
        analysis_date,
        get_pandas = True,
        crs = PROJECT_CRS
    )
    
    logger.info("grab all stops")
    
    # Create hqta_type == major_stop_bus
    major_stop_bus = create_major_stop_bus(all_stops, bus_intersections)
    logger.info("create major stop bus")

    # Create hqta_type = hq_corridor_bus
    stops_in_hq_corr = create_stops_along_corridors(all_stops)
    logger.info("create hq corridor bus")
    
    # Export to GCS    
    utils.geoparquet_gcs_export(
        major_stop_bus, 
        GCS_FILE_PATH,
        'major_stop_bus'
    )
    
    utils.geoparquet_gcs_export(
        stops_in_hq_corr,
        GCS_FILE_PATH,
        'stops_in_hq_corr'
    )
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    #client.close()