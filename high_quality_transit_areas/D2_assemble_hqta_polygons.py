"""
Make a polygon version of HQ transit corridors for open data portal.

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import intake
import pandas as pd
import sys

from loguru import logger

import C1_prep_pairwise_intersections as prep_clip
import D1_assemble_hqta_points as assemble_hqta_points
import utilities
from shared_utils import utils, geography_utils
from D1_assemble_hqta_points import EXPORT_PATH, add_route_info
from update_vars import analysis_date

logger.add("./logs/D2_assemble_hqta_polygons.log", retention="6 months")
logger.add(sys.stderr, 
           format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
           level="INFO")

HQTA_POINTS_FILE = utilities.catalog_filepath("hqta_points")
catalog = intake.open_catalog("*.yml")

## drop incorrect Half Moon Bay data, TODO investigate
## drop incorrect Cheviot Hills (Motor Ave, south of Pico) showing up data, TODO investigate refactor (run shapes in frequency order...)

# check3_hqta_points...first 2 stops don't exist
# 689 is ok, shows up for multiple operators, but where it's landing is ok
# bad_stops = ['315604', '315614', '689']

bad_stops = []

def get_dissolved_hq_corridor_bus(gdf: dg.GeoDataFrame) -> dg.GeoDataFrame:
    """
    Take each segment, then dissolve by operator,
    and use this dissolved polygon in hqta_polygons.
    
    Draw a buffer around this.
    """
    # Can keep route_id in dissolve, but route_id is not kept in final 
    # export, so there would be multiple rows for multiple route_ids, 
    # and no way to distinguish between them
    keep_cols = ['calitp_itp_id', 'hq_transit_corr', 'route_id']
    
    gdf2 = gdf[keep_cols + ['geometry']].compute()
    
    dissolved = (gdf2.dissolve(by=keep_cols)
                 .reset_index()
                )
    
    # Turn back into dask gdf
    dissolved_gddf = dg.from_geopandas(dissolved, npartitions=1)

    return dissolved_gddf


def filter_and_buffer(hqta_points: dg.GeoDataFrame, 
                      hqta_segments: dg.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Convert the HQTA point geom into polygon geom.
    
    Buffers are already drawn for corridors and stops, so 
    draw new buffers, and address each hqta_type separately.
    """
    stops = (hqta_points[hqta_points.hqta_type != "hq_corridor_bus"]
             .to_crs(geography_utils.CA_NAD83Albers)
            )
    
    corridor_segments = hqta_segments.to_crs(geography_utils.CA_NAD83Albers)
    corridors = get_dissolved_hq_corridor_bus(corridor_segments)
    
    # General buffer distance: 1/2mi ~= 805 meters
    # Bus corridors are already buffered 100 meters, so will buffer 705 meters
    stops = stops.assign(
        geometry = stops.geometry.buffer(705)
    )
    
    corridor_cols = [
        "calitp_itp_id_primary", "hqta_type", "route_id", "geometry"
    ]
    
    corridors = corridors.assign(
        geometry = corridors.geometry.buffer(755),
        # overwrite hqta_type for this polygon
        hqta_type = "hq_corridor_bus",
        calitp_itp_id_primary = corridors.calitp_itp_id.astype(int),
    )[corridor_cols]
    
    corridors["hqta_details"] = corridors.apply(
        utilities.hqta_details, meta=("hqta_details", "str"), 
        axis=1)
    
    
    hqta_polygons = (dd.multi.concat([corridors, stops], axis=0)
                     .to_crs(geography_utils.WGS84)
                     # Make sure dtype is still int after concat
                     .astype({"calitp_itp_id_primary": int})
                    ).compute()
    
    return hqta_polygons


def drop_bad_stops_final_processing(gdf: gpd.GeoDataFrame, 
                                    bad_stop_list: list) -> gpd.GeoDataFrame:
    """
    Input a list of known bad stops in the script to 
    exclude from the polygons.
    """
    
    keep_cols = [
        "calitp_itp_id_primary", "calitp_itp_id_secondary", 
        "agency_name_primary", "agency_name_secondary",
        "hqta_type", "hqta_details", "route_id", "geometry"
    ]
    
    # Drop bad stops, subset columns
    gdf2 = (gdf[~gdf.stop_id.isin(bad_stop_list)]
            [keep_cols]
            .sort_values(["hqta_type", "calitp_itp_id_primary", 
                          "calitp_itp_id_secondary",
                          "hqta_details", "route_id"])
            .reset_index(drop=True)
           )
    
    return gdf2


if __name__=="__main__":
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()
    
    hqta_points = dg.read_parquet(HQTA_POINTS_FILE)
    bus_hq_corr = prep_clip.prep_bus_corridors()
    
    # Filter and buffer for stops (805 m) and corridors (755 m)
    # and add agency_names
    gdf = filter_and_buffer(hqta_points, bus_hq_corr)
    
    time1 = dt.datetime.now()
    logger.info(f"filter and buffer: {time1 - start}")
    
    # Drop bad stops, subset, get ready for export
    gdf2 = drop_bad_stops_final_processing(gdf, bad_stops)    
    
    # Export to GCS
    # Stash this date's into its own folder, to convert to geojson, geojsonl
    utils.geoparquet_gcs_export(gdf2, 
                                EXPORT_PATH, 
                                'ca_hq_transit_areas'
                               )
    logger.info("export as geoparquet in date folder")

    # Overwrite most recent version (other catalog entry constantly changes)
    utils.geoparquet_gcs_export(gdf2,
                                utilities.GCS_FILE_PATH,
                                'hqta_areas'
                               )    
    
    logger.info("export as geoparquet")
    
    # Add geojson / geojsonl exports
    utils.geojson_gcs_export(gdf2, 
                             EXPORT_PATH,
                             'ca_hq_transit_areas', 
                             geojson_type = "geojson"
                            )
    
    logger.info("export as geojson")

    utils.geojson_gcs_export(gdf2, 
                         EXPORT_PATH,
                         'ca_hq_transit_areas', 
                         geojson_type = "geojsonl"
                        )
    
    logger.info("export as geojsonl")
        
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")