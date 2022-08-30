"""
Create the bus-related hqta_types

These are hqta_types: 
* major_stop_bus: the bus stop within the above intersection does not necessarily have
the highest trip count
* hq_corridor_bus: stops along the HQ transit corr (may not be highest trip count)

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import pandas as pd
import sys

from calitp.tables import tbl
from siuba import *
from loguru import logger

import C1_prep_for_clipping as prep_clip
import C3_clean_clipped_intersections as clean_clip
from shared_utils import utils, geography_utils, gtfs_utils
from utilities import catalog_filepath
from update_vars import analysis_date

logger.add("./logs/C4_create_bus_hqta_types.log")
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")

# Input files
ALL_BUS = catalog_filepath("all_bus")

segment_cols = ["calitp_itp_id", "hqta_segment_id"]
stop_cols = ["calitp_itp_id", "stop_id"]


def query_all_stops(analysis_date: dt.date) -> gpd.GeoDataFrame:
    """
    Query and find all stops on selected date 
    for all operators (except ITP ID 200)
    and return a gpd.GeoDataFrame.
    """
    keep_stop_cols = [
        "calitp_itp_id", "stop_id", "stop_lon", "stop_lat"
    ]
    
    ALL_IDS_NO_200 = (tbl.gtfs_schedule.agency()
         >> distinct(_.calitp_itp_id)
         >> filter(_.calitp_itp_id != 200)
         >> collect()
        ).calitp_itp_id.tolist()
    
    stops = gtfs_utils.get_stops(
        selected_date = analysis_date,
        itp_id_list = ALL_IDS_NO_200,
        stop_cols = keep_stop_cols,
        get_df = True,
        crs = geography_utils.CA_NAD83Albers,
        custom_filtering = None
    )
    
    return stops


def merge_clipped_with_hqta_segments() -> gpd.GeoDataFrame:
    """
    Merge the cleaned up clipped areas (large areas dropped) 
    with hqta segments (which contains stop_id).
    
    Use an inner merge because we only want the operators
    who have stops that appear in clipped bus corridor intersections.
    These will be called "frequent bus stops in intersections".
    
    Return a gpd.GeoDataFrame because we need to use gdf.explode(),
    available only in geopandas, not dask_geopandas.
    """
    # Clean up the clipped df and remove large shapes
    clipped_df = clean_clip.process_clipped_intersections()
    
    # For hqta_segment level data, only 1 stop is attached to each segment
    # It's the stop with the highest trip count
    hqta_segment = dg.read_parquet(ALL_BUS)
    
    keep_cols = segment_cols + ["stop_id", "hq_transit_corr"]

    stops_in_bus_intersections = dd.merge(
        # Put clipped geometries on the left for merge
        # Keep the clipped geom, don't want the full hqta_segment geom
        clipped_df,
        hqta_segment[keep_cols],
        on = segment_cols,
        how = "inner"
    )
    
    # Change to gdf because gdf.explode only works with geopandas, not dask gdf
    return stops_in_bus_intersections.compute()


def explode_bus_intersections(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Explode so that the multipolygon becomes multiple rows of polygons
    
    Each polygon contains one clipped area
    Explode works with geopandas gdf, but not dask gdf
    """
    keep_cols = segment_cols + ["stop_id", "hq_transit_corr", "geometry"]
    
    one_intersection_per_row = gdf[keep_cols].explode(ignore_index=True)
    
    return one_intersection_per_row


def only_major_bus_stops(all_stops: gpd.GeoDataFrame, 
                         bus_intersections: gpd.GeoDataFrame):
    """
    Pare down all stops to include only the operators
    who have stops that show up in bus corridor intersections (clipped results).
    """
    all_stops_for_major_operators = dd.merge(
        all_stops,
        bus_intersections[["calitp_itp_id"]].drop_duplicates(),
        on = "calitp_itp_id",
        how = "inner"
    )
    
    return all_stops_for_major_operators


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
    # that have at least 1 stop that is in freq_bus_stops
    major_stops = only_major_bus_stops(all_stops, bus_intersections)

    major_bus_stops_in_intersections = (
        dg.sjoin(
            major_stops,
            bus_intersections[["calitp_itp_id", "geometry"]],
            how = "inner",
            predicate = "within"
        ).drop(columns = "index_right")
        .drop_duplicates(
            subset=["calitp_itp_id_left", "stop_id", "calitp_itp_id_right"])
    ).reset_index(drop=True)
    
    stops_in_intersection = (
        major_bus_stops_in_intersections.assign(
            hqta_type = "major_stop_bus",
            ).rename(columns = 
                     {"calitp_itp_id_left": "calitp_itp_id_primary", 
                      "calitp_itp_id_right": "calitp_itp_id_secondary",
                     })
          [["calitp_itp_id_primary", "calitp_itp_id_secondary", 
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
    bus_corridors = prep_clip.prep_bus_corridors()
    
    stops_in_hq_corr = (dg.sjoin(
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
                         .rename(columns = {"calitp_itp_id": "calitp_itp_id_primary"})
                        )
    
    return stops_in_hq_corr2


if __name__ == "__main__":
    
    start = dt.datetime.now()
    
    # Narrow down to the stops of operators who also have
    # stops in the clipped results.
    freq_bus_stops_in_intersections = merge_clipped_with_hqta_segments()

    # This exploded geometry is what will be used in spatial join on all stops
    # to see which stops fall in this
    bus_intersections = explode_bus_intersections(freq_bus_stops_in_intersections)
    
    # Grab point geom with all stops
    all_stops = query_all_stops(analysis_date)
    logger.info("grab all stops")
    
    # Create hqta_type == major_stop_bus
    major_stop_bus = create_major_stop_bus(all_stops, bus_intersections)
    logger.info("create major stop bus")

    # Create hqta_type = hq_corridor_bus
    stops_in_hq_corr = create_stops_along_corridors(all_stops)
    logger.info("create hq corridor bus")
    
    # Export locally
    # Remove in the following script after appending
    major_stop_bus.compute().to_parquet("./data/major_stop_bus.parquet")
    stops_in_hq_corr.compute().to_parquet("./data/stops_in_hq_corr.parquet")
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")