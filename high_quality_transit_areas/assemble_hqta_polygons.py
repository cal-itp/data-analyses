"""
Make a polygon version of HQ transit corridors for open data portal.

From combine_and_visualize.ipynb
"""
import datetime
import geopandas as gpd
import intake
import pandas as pd
import sys

from loguru import logger

import _utils
from prep_pairwise_intersections import prep_bus_corridors
from assemble_hqta_points import get_agency_crosswalk
from calitp_data_analysis import utils, geography_utils
from update_vars import (GCS_FILE_PATH, analysis_date, PROJECT_CRS, EXPORT_PATH,
                         HALF_MILE_BUFFER_METERS, CORRIDOR_BUFFER_METERS
                        )

catalog = intake.open_catalog("*.yml")

def buffer_hq_corridor_bus(
    analysis_date: str,
    buffer_meters: int,
) -> gpd.GeoDataFrame:
    """
    Buffer hq bus corridors.
    
    Start with bus corridors, filter to those that are high quality,
    and do a dissolve.
    After the dissolve, buffer by an additional amount to 
    get the full 0.5 mile buffer.
    """
    gdf = prep_bus_corridors(
        is_hq_corr=True
    ).to_crs(PROJECT_CRS)
    
    keep_cols = ['schedule_gtfs_dataset_key', 'route_id']
    
    dissolved = (gdf[keep_cols + ["geometry"]]
                 .dissolve(by=keep_cols)
                 .reset_index()
                )
    
    # Bus corridors are already buffered 50 meters, 
    # so will buffer 705 meters to get 0.5 mile radius
    corridors = dissolved.assign(
        geometry = dissolved.geometry.buffer(buffer_meters),
        # overwrite hqta_type for this polygon
        hqta_type = "hq_corridor_bus",
    ).pipe(_utils.primary_rename)
    
    agency_info = get_agency_crosswalk(analysis_date)
    
    # Make sure gtfs_dataset_name and organization columns are added
    corridors2 = pd.merge(
        corridors,
        agency_info.add_suffix("_primary"),
        on = "schedule_gtfs_dataset_key_primary",
        how = "inner"
    )
    
    corridors2 = corridors2.assign(
        hqta_details = "stop_along_hq_bus_corridor_single_operator"
    )
    
    return corridors2


def buffer_major_transit_stops(
    buffer_meters: int
) -> gpd.GeoDataFrame:
    """
    Buffer major transit stops. 
    Start with hqta points and filter out the hq_corridor_bus types.
    """
    hqta_points = catalog.hqta_points.read().to_crs(PROJECT_CRS)

    stops = hqta_points[hqta_points.hqta_type != "hq_corridor_bus"]
    
    # General buffer distance: 1/2mi ~= 805 meters
    # stops are already buffered 
    stops = stops.assign(
        geometry = stops.geometry.buffer(buffer_meters)
    )

    return stops


def combine_corridors_and_stops(
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Convert the HQTA point geom into polygon geom.
    
    Buffers are already drawn for corridors and stops, so 
    draw new buffers, and address each hqta_type separately.
    """
    corridors = buffer_hq_corridor_bus(
        analysis_date,
        buffer_meters = CORRIDOR_BUFFER_METERS,
    )
    
    major_transit_stops = buffer_major_transit_stops(
        buffer_meters = HALF_MILE_BUFFER_METERS
    )
    
    hqta_polygons = pd.concat([
        corridors, 
        major_transit_stops
    ], axis=0).to_crs(geography_utils.WGS84)
    
    return hqta_polygons


def final_processing(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Drop extra columns, get sorting done.
    """    
    keep_cols = [
        "agency_primary", "agency_secondary",
        "hqta_type", "hqta_details", "route_id", 
        "base64_url_primary", "base64_url_secondary",
        "org_id_primary", "org_id_secondary",
        "geometry"
    ]
    
    gdf2 = (
        gdf[keep_cols]
            .drop_duplicates()
            .sort_values(["hqta_type", "agency_primary", 
                          "agency_secondary",
                          "hqta_details", "route_id"])
            .reset_index(drop=True)
           )
    
    return gdf2


if __name__=="__main__":    
    
    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    start = datetime.datetime.now()
    
    gdf = combine_corridors_and_stops(analysis_date).pipe(final_processing)
       
    # Export to GCS
    utils.geoparquet_gcs_export(
        gdf, 
        EXPORT_PATH, 
        "ca_hq_transit_areas"
    )
    
    # Overwrite most recent version (other catalog entry constantly changes)
    utils.geoparquet_gcs_export(
        gdf,
        GCS_FILE_PATH,
        "hqta_areas"
    )    
       
    end = datetime.datetime.now()
    logger.info(
        f"D2_assemble_hqta_polygons {analysis_date} "
        f"execution time: {end - start}"
    )
    