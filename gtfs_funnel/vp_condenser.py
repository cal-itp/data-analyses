"""
Condense vp into arrays by trip-direction.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import vp_transform, wrangle_shapes
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS

def condense_vp_to_linestring(
    analysis_date: str, 
    dict_inputs: dict
):
    """
    Turn vp (df with point geometry) into a condensed 
    linestring version.
    We will group by trip and save out 
    the vp point geom into a shapely.LineString.
    """
    USABLE_VP = dict_inputs.speeds_tables.vp_dwell
    EXPORT_FILE = dict_inputs.speeds_tables.vp_condensed_line
    
    vp = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}",
        columns = ["trip_instance_key", "x", "y", 
                   "vp_idx", "vp_primary_direction", 
                   "location_timestamp_local", 
                   "moving_timestamp_local",
                  ],
    ).pipe(wrangle_shapes.vp_as_gdf, crs = WGS84)
        
    vp_condensed = delayed(vp_transform.condense_point_geom_to_line)(
        vp,
        group_cols = ["trip_instance_key"],
        geom_col = "geometry",
        other_cols = ["vp_idx", "location_timestamp_local", 
                      "moving_timestamp_local",
                      "vp_primary_direction",
                     ],
    ).set_geometry("geometry").set_crs(WGS84)
    
    vp_condensed = compute(vp_condensed)[0]
    
    utils.geoparquet_gcs_export(
        vp_condensed,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
        
    return 


def prepare_vp_for_all_directions(
    analysis_date: str, 
    dict_inputs: dict
) -> gpd.GeoDataFrame:
    """
    For each direction, exclude one the opposite direction and
    save out the arrays of valid indices.
    Every trip will have 4 rows, 1 row corresponding to each direction.
    
    Ex: for a given trip's northbound points, exclude southbound vp.
    Subset vp_idx, location_timestamp_local and coordinate arrays 
    to exclude southbound.
    """
    INPUT_FILE = dict_inputs.speeds_tables.vp_condensed_line
    EXPORT_FILE = dict_inputs.speeds_tables.vp_nearest_neighbor
    
    vp = delayed(gpd.read_parquet)(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
    )
  
    dfs = [
        delayed(vp_transform.combine_valid_vp_for_direction)(
            vp, direction) 
        for direction in wrangle_shapes.ALL_DIRECTIONS
    ]
        
    results = [compute(i)[0] for i in dfs]

    gdf = pd.concat(
        results, axis=0, ignore_index=True
    ).sort_values(
        ["trip_instance_key", "vp_primary_direction"]
    ).reset_index(drop=True)
        
    utils.geoparquet_gcs_export(
        gdf,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
    
    return 


if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    LOG_FILE = "./logs/vp_preprocessing.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
        
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()
        
        condense_vp_to_linestring(analysis_date, GTFS_DATA_DICT)
        
        time1 = datetime.datetime.now()
        
        logger.info(
            f"{analysis_date}: condense vp for trip "
            f"{time1 - start}"
        )
        
        prepare_vp_for_all_directions(analysis_date, GTFS_DATA_DICT)
        
        end = datetime.datetime.now()
        logger.info(
            f"{analysis_date}: prepare vp to use in nearest neighbor: "
            f"{end - time1}"
        )       