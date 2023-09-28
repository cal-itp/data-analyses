import dask_geopandas as dg
import gcsfs
import geopandas as gpd
import intake
import pandas as pd
import os
import shapely

from typing import Union
from calitp_data_analysis import calitp_color_palette

fs = gcsfs.GCSFileSystem()

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/high_quality_transit_areas"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

catalog = intake.open_catalog("./*.yml")

# Colors
BLUE = "#08589e"
ORANGE = "#fec44f"
ITP_BLUE = calitp_color_palette.CALITP_CATEGORY_BOLD_COLORS[0]


# Fill out HQTA details of why nulls are present
# based on feedback from open data publishing process
# Concise df can be confusing to users if they don't know how to interpret presence of nulls
# and which cases of HQTA definitions those correspond to
def hqta_details(row):
    if row.hqta_type == "major_stop_bus":
        if row.feed_key_primary != row.feed_key_secondary:
            return "intersection_2_bus_routes_different_operators"
        else:
            return "intersection_2_bus_routes_same_operator"
    elif row.hqta_type == "hq_corridor_bus":
        return "stop_along_hq_bus_corridor_single_operator"
    elif row.hqta_type in ["major_stop_ferry", 
                           "major_stop_brt", "major_stop_rail"]:
        # (not sure if ferry, brt, rail, primary/secondary ids are filled in.)
        return row.hqta_type + "_single_operator"
    
    
def clip_to_ca(gdf: Union[gpd.GeoDataFrame, dg.GeoDataFrame]
              ) -> Union[gpd.GeoDataFrame, dg.GeoDataFrame]:
    """
    Clip to CA boundaries. 
    Can take dask gdf or geopandas gdf
    """
    
    EXISTING_EPSG = gdf.crs.to_epsg()
    
    ca = catalog.ca_boundary.read().to_crs(f"EPSG: {EXISTING_EPSG}")

    if isinstance(gdf, gpd.GeoDataFrame):
        gdf2 = gdf.clip(ca, keep_geom_type = False)
    
    elif isinstance(gdf, dg.GeoDataFrame):
        gdf2 = dg.clip(gdf, ca, keep_geom_type = False)

    return gdf2

    
def catalog_filepath(file: str) -> str:
    """
    Return the file path for files in catalog.
    Need full path when using dask to read in files, 
    can't just use catalog.file_name.read().
    """
    return catalog[file].urlpath