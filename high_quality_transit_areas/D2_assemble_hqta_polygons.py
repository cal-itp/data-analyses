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

import C1_prep_pairwise_intersections as prep_clip
import D1_assemble_hqta_points as assemble_hqta_points
from calitp_data_analysis import utils, geography_utils
from D1_assemble_hqta_points import (EXPORT_PATH, add_route_info)
from update_vars import GCS_FILE_PATH, analysis_date, PROJECT_CRS

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
    )[corridor_cols].rename(
        columns = {"feed_key": "feed_key_primary"}
    )
    
    crosswalk = pd.read_parquet(
        f"{GCS_FILE_PATH}feed_key_org_crosswalk.parquet"
    )
    primary_agency_cols = [i for i in crosswalk.columns if "_primary" in i]
    
    crosswalk = crosswalk[primary_agency_cols].drop_duplicates()
    
    corridors2 = pd.merge(
        corridors,
        crosswalk,
        on = "feed_key_primary",
        how = "inner"
    )
    
    corridors2 = corridors2.assign(
        hqta_details = corridors2.apply(
            assemble_hqta_points.hqta_details, axis=1),
    )

    return corridors2


def filter_and_buffer(
    hqta_points: gpd.GeoDataFrame, 
    hqta_segments: gpd.GeoDataFrame, 
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Convert the HQTA point geom into polygon geom.
    
    Buffers are already drawn for corridors and stops, so 
    draw new buffers, and address each hqta_type separately.
    """
    stops = hqta_points[hqta_points.hqta_type != "hq_corridor_bus"]
    
    corridors = get_dissolved_hq_corridor_bus(hqta_segments, analysis_date)
    
    # General buffer distance: 1/2mi ~= 805 meters
    # Bus corridors are already buffered 100 meters, so will buffer 705 meters
    stops = stops.assign(
        geometry = stops.geometry.buffer(705)
    )
    
    hqta_polygons = pd.concat([
        corridors, 
        stops
    ], axis=0).to_crs(geography_utils.WGS84)
    
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
    
    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    start = datetime.datetime.now()
    
    hqta_points = catalog.hqta_points.read().to_crs(PROJECT_CRS)
    bus_hq_corr = prep_clip.prep_bus_corridors(
        is_hq_corr=True
    ).to_crs(PROJECT_CRS)
    
    # Filter and buffer for stops (805 m) and corridors (755 m)
    # and add agency_names
    gdf = filter_and_buffer(
        hqta_points, bus_hq_corr, analysis_date
    ).pipe(final_processing)
            
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
    logger.info(f"D2_assemble_hqta_polygons {analysis_date} "
                f"execution time: {end - start}")