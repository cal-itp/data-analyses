"""
Project vp point geometry onto shape.
Condense vp into linestring.
"""
import datetime
import pandas as pd
import geopandas as gpd
import sys

from loguru import logger
from calitp_data_analysis import utils

from segment_speed_utils import helpers, vp_transform
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import rt_dates, geo_utils
import model_utils


def create_vp_with_shape_and_project(
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Take usable vp, convert into gdf, and project each vp
    against shape.
    """
    VP_FILE = GTFS_DATA_DICT.modeled_vp.raw_vp
    
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}{VP_FILE}_{analysis_date}.parquet",
        columns = ["trip_instance_key", "location_timestamp_local", "geometry"],
    ).to_crs(PROJECT_CRS)
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_id", "trip_instance_key", "shape_array_key"],
        get_pandas = True,

    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        get_pandas = True,
        crs = PROJECT_CRS,
        filters = [[("shape_array_key", "in", trips.shape_array_key.tolist())]]
    ).merge(
        trips,
        on = "shape_array_key",
        how = "inner"
    )
    
    vp_gdf = pd.merge(
        vp.rename(columns = {"geometry": "vp_geometry"}),
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "trip_instance_key",
        how = "inner"
    ).set_geometry("vp_geometry")
    
    vp_gdf = model_utils.project_point_onto_shape(
        vp_gdf, 
        "shape_geometry", 
        "vp_geometry"
    ).rename(columns = {"projected_meters": "vp_meters"})
    
    return vp_gdf


if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_vp_preprocessing.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    analysis_date_list = [
        rt_dates.DATES["oct2024"]
    ]
    
    for analysis_date in analysis_date_list:
        
        start = datetime.datetime.now()

        EXPORT_FILE = GTFS_DATA_DICT.modeled_vp.vp_projected

        gdf = create_vp_with_shape_and_project(
            analysis_date
        ).pipe(
            vp_transform.condense_point_geom_to_line,
            group_cols = ["trip_instance_key"],
            geom_col = "vp_geometry",
            array_cols = ["vp_meters", "location_timestamp_local"],
            sort_cols = ["trip_instance_key", "location_timestamp_local"]
        ).set_geometry("vp_geometry").set_crs(PROJECT_CRS)

        utils.geoparquet_gcs_export(
            gdf,
            SEGMENT_GCS,
            f"{EXPORT_FILE}_{analysis_date}"
        )
    
        end = datetime.datetime.now()
        
        logger.info(
            f"{analysis_date} project vp against shape and condense: {end - start}"
        )
    