"""
Do linear referencing by segment-trip 
and derive speed.
"""
import os
os.environ['USE_PYGEOS'] = '0'

import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import pandas as pd
import sys

from loguru import logger

from shared_utils.geography_utils import WGS84
from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS, CONFIG_PATH)    
    
def linear_referencing_and_speed_by_segment(
    analysis_date: str,
    dict_inputs: dict = {}
):
    """
    With just enter / exit points on segments, 
    do the linear referencing to get shape_meters, and then derive speed.
    """
    time0 = datetime.datetime.now()    
    
    VP_FILE = dict_inputs["stage3"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]    
    EXPORT_FILE = dict_inputs["stage4"]
    
    # Keep subset of columns - don't need it all. we can get the 
    # columns dropped through segments file
    vp_keep_cols = [
        'gtfs_dataset_key', 'gtfs_dataset_name', 
        'trip_id', 'trip_instance_key',
        'schedule_gtfs_dataset_key',
        TIMESTAMP_COL,
        'x', 'y'
    ] + SEGMENT_IDENTIFIER_COLS
    
    vp = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        f"{VP_FILE}_{analysis_date}/",
        file_type = "df",
        columns = vp_keep_cols,
        partitioned = True
    )
    
    segments = helpers.import_segments(
        SEGMENT_GCS,
        f"{SEGMENT_FILE}_{analysis_date}", 
        columns = SEGMENT_IDENTIFIER_COLS + ["geometry"]
    ).dropna(subset="geometry").reset_index(drop=True)
     
    # https://stackoverflow.com/questions/71685387/faster-methods-to-create-geodataframe-from-a-dask-or-pandas-dataframe
    # https://github.com/geopandas/dask-geopandas/issues/197    
    vp_gddf = dg.from_dask_dataframe(
        vp, 
        geometry=dg.points_from_xy(vp, "x", "y")
    ).set_crs(WGS84).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    vp_with_seg_geom = dd.merge(
        vp_gddf, 
        segments,
        on = SEGMENT_IDENTIFIER_COLS,
        how = "inner"
    ).rename(columns = {
        "geometry_x": "vp_geometry",
        "geometry_y": "segment_geometry"}
    ).set_geometry("vp_geometry")

    vp_with_seg_geom = vp_with_seg_geom.repartition(npartitions=50)
          
    time1 = datetime.datetime.now()
    logger.info(f"set up merged vp with segments: {time1 - time0}")
    
    shape_meters_series = vp_with_seg_geom.map_partitions(
        wrangle_shapes.project_point_geom_onto_linestring,
        "segment_geometry",
        "vp_geometry",
        meta = ("shape_meters", "float")
    )
    
    vp_with_seg_geom["shape_meters"] = shape_meters_series
    vp_with_seg_geom = segment_calcs.convert_timestamp_to_seconds(
        vp_with_seg_geom, [TIMESTAMP_COL])
    
    time2 = datetime.datetime.now()
    logger.info(f"linear referencing: {time2 - time1}")
   
    # set up metadata for columns in exact order output appears for map_partitions
    dtypes_dict = vp_with_seg_geom[
        ["gtfs_dataset_key", "gtfs_dataset_name", 
         "trip_id", "trip_instance_key", 
         "schedule_gtfs_dataset_key"
        ] + SEGMENT_IDENTIFIER_COLS
    ].dtypes.to_dict()

    speeds = vp_with_seg_geom.map_partitions(
        segment_calcs.calculate_speed_by_segment_trip,
        SEGMENT_IDENTIFIER_COLS,
        f"{TIMESTAMP_COL}_sec",
        meta = {
            **dtypes_dict,
            "min_time": "float",
            "min_dist": "float",
            "max_time": "float",
            "max_dist": "float",
            "meters_elapsed": "float", 
            "sec_elapsed": "float",
            "speed_mph": "float",
        }) 
    
    speeds = speeds.repartition(npartitions=2)
    
    time3 = datetime.datetime.now()
    logger.info(f"calculate speeds: {time3 - time2}")
    
    speeds.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}", 
        overwrite = True
    )

    
if __name__ == "__main__": 
    
    LOG_FILE = "../logs/speeds_by_segment_trip.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    linear_referencing_and_speed_by_segment(
        analysis_date, 
        dict_inputs = STOP_SEG_DICT
    )
            
    logger.info(f"speeds for stop segments: {datetime.datetime.now() - start}")
    logger.info(f"execution time: {datetime.datetime.now() - start}")
            