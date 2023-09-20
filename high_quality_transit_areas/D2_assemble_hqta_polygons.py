"""
Make a polygon version of HQ transit corridors for open data portal.

From combine_and_visualize.ipynb
"""
import datetime as dt
import dask_geopandas as dg
import geopandas as gpd
import intake
import pandas as pd
import sys

from calitp_data_analysis import get_fs
from loguru import logger

import C1_prep_pairwise_intersections as prep_clip
import D1_assemble_hqta_points as assemble_hqta_points
import utilities
from shared_utils import utils, geography_utils
from D1_assemble_hqta_points import (EXPORT_PATH, add_route_info)
from update_vars import analysis_date

fs = get_fs()

catalog = intake.open_catalog("*.yml")

def get_dissolved_hq_corridor_bus(
    gdf: gpd.GeoDataFrame, 
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Take each segment, then dissolve by operator,
    and use this dissolved polygon in hqta_polygons.
    
    Draw a buffer around this.
    """
    # Can keep route_id in dissolve, but route_id is not kept in final 
    # export, so there would be multiple rows for multiple route_ids, 
    # and no way to distinguish between them
    keep_cols = ['feed_key', 'hq_transit_corr', 'route_id']
    
    dissolved = (gdf[keep_cols + ["geometry"]]
                 .dissolve(by=keep_cols)
                 .reset_index()
                )
    
    # For hq_corridor_bus, we have feed_key again, and need to 
    # add agency_name, or else this category will have missing name values
    corridor_cols = [
        "feed_key", "hqta_type", "route_id", "geometry"
    ]
    corridors = dissolved.assign(
        geometry = dissolved.geometry.buffer(755),
        # overwrite hqta_type for this polygon
        hqta_type = "hq_corridor_bus",
    )[corridor_cols]
    
    feeds_df = corridors[["feed_key"]].drop_duplicates()
    crosswalk = assemble_hqta_points.get_agency_info(feeds_df, analysis_date)
        
    NAMES_DICT = dict(zip(crosswalk.feed_key, crosswalk.organization_name))
    B64_DICT = dict(zip(crosswalk.feed_key, crosswalk.base64_url))
    ORG_DICT = dict(zip(crosswalk.feed_key, crosswalk.organization_source_record_id))  
    
    corridors = corridors.assign(
        agency_primary = corridors.feed_key.map(NAMES_DICT),
        hqta_details = corridors.apply(utilities.hqta_details, axis=1),
        org_id_primary = corridors.feed_key.map(ORG_DICT),
        base64_url_primary = corridors.feed_key.map(B64_DICT),
    ).rename(columns = {"feed_key": "feed_key_primary"})

    return corridors


def filter_and_buffer(hqta_points: gpd.GeoDataFrame, 
                      hqta_segments: dg.GeoDataFrame, 
                      analysis_date: str
                     ) -> gpd.GeoDataFrame:
    """
    Convert the HQTA point geom into polygon geom.
    
    Buffers are already drawn for corridors and stops, so 
    draw new buffers, and address each hqta_type separately.
    """
    stops = (hqta_points[hqta_points.hqta_type != "hq_corridor_bus"]
             .to_crs(geography_utils.CA_NAD83Albers)
            )
    
    corridor_segments = hqta_segments.to_crs(
        geography_utils.CA_NAD83Albers).compute()
    
    corridors = get_dissolved_hq_corridor_bus(corridor_segments, analysis_date)
    
    # General buffer distance: 1/2mi ~= 805 meters
    # Bus corridors are already buffered 100 meters, so will buffer 705 meters
    stops = stops.assign(
        geometry = stops.geometry.buffer(705)
    )
    
    hqta_polygons = (
        pd.concat([
            corridors, 
            stops
        ], axis=0)
        .to_crs(geography_utils.WGS84)
    )
    
    return hqta_polygons


def final_processing(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Drop extra columns, get sorting done.
    Used to drop bad stops, but these all look ok.
    """
    
    keep_cols = [
        "agency_primary", "agency_secondary",
        "hqta_type", "hqta_details", "route_id", 
        "base64_url_primary", "base64_url_secondary",
        "org_id_primary", "org_id_secondary",
        "geometry"
    ]
    
    # Drop bad stops, subset columns
    gdf2 = (gdf[keep_cols]
            .drop_duplicates()
            .sort_values(["hqta_type", "agency_primary", 
                          "agency_secondary",
                          "hqta_details", "route_id"])
            .reset_index(drop=True)
           )
    
    return gdf2


if __name__=="__main__":    
    
    logger.add("./logs/D2_assemble_hqta_polygons.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
    hqta_points = catalog.hqta_points.read()
    bus_hq_corr = prep_clip.prep_bus_corridors()
    
    # Filter and buffer for stops (805 m) and corridors (755 m)
    # and add agency_names
    gdf = filter_and_buffer(hqta_points, bus_hq_corr, analysis_date)
    
    time1 = dt.datetime.now()
    logger.info(f"filter and buffer: {time1 - start}")
    
    # Subset, get ready for export
    gdf = final_processing(gdf)    
    
    # Export to GCS
    # Stash this date's into its own folder, to convert to geojson, geojsonl
    utils.geoparquet_gcs_export(
        gdf, 
        EXPORT_PATH, 
        'ca_hq_transit_areas'
    )
    
    logger.info("export as geoparquet in date folder")
    
    # Overwrite most recent version (other catalog entry constantly changes)
    utils.geoparquet_gcs_export(
        gdf,
        utilities.GCS_FILE_PATH,
        'hqta_areas'
    )    
    
    logger.info("export as geoparquet")
        
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")