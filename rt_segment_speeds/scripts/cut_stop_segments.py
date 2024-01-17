"""
Cut stop-to-stop segments for all trips.
Use one of gtfs_segments functions to do it...
it cuts the segments, particularly loop_or_inlining
shapes better at the edges.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import gtfs_segments
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              PROJECT_CRS, 
                                              CONFIG_PATH
                                             )

def stop_times_with_shape(
    analysis_date: str
) -> dg.GeoDataFrame: 
    """
    Filter down to trip_instance_keys present in vp,
    and attach stop_times and shapes.
    Set up this df the way we need to use gtfs_segments.create_segments.
    """
    rt_trips = helpers.import_unique_vp_trips(analysis_date)
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key",
                   "shape_array_key",
                   "stop_id", "stop_sequence", "geometry"],
        filters = [[("trip_instance_key", "in", rt_trips)]],
        with_direction = True,
        get_pandas = False,
        crs = "EPSG:4326"
    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = "EPSG:4326",
        get_pandas = True
    ).dropna(subset="geometry")
    
    df = dd.merge(
        stop_times,
        shapes,
        on = "shape_array_key",
        how = "inner"
    ).rename(columns = {
        "geometry_x": "start",
        "geometry_y": "geometry",
        "trip_instance_key": "trip_id", 
        "shape_array_key": "shape_id",
    }).dropna(
        subset="geometry"
    ).reset_index(drop=True).set_geometry("geometry")
    
    return df


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date
    
    LOG_FILE = "../logs/cut_stop_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    EXPORT_FILE = STOP_SEG_DICT["segments_file"]
    
    ddf = stop_times_with_shape(analysis_date)
    
    # This is a lot of 
    ddf = ddf.repartition(npartitions=150).persist()
        
    renamed_ddf = ddf.rename(columns = {"stop_id": "stop_id1"})
    orig_dtypes = renamed_ddf.dtypes.to_dict()

    segments = ddf.map_partitions(
        gtfs_segments.gtfs_segments.create_segments,
        meta = {
            **orig_dtypes,
            "snap_start_id": "int", 
            "stop_id2": "str", 
            "end": "geometry",
            "snap_end_id": "int", 
            "segment_id": "str"
        },
        align_dataframes = False
    )

    # We don't need several of these columns, esp 3 geometry columns
    segments = (segments.drop(
        columns = ["start", "end", 
                   "snap_start_id", "snap_end_id"]
    ).rename(columns = {
        "shape_id": "shape_array_key",
        "trip_id": "trip_instance_key",
    }).set_geometry("geometry")
      .set_crs("EPSG:4326")
     .to_crs(PROJECT_CRS)
     .compute()
     )
    
    utils.geoparquet_gcs_export(
        segments,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    logger.info(f"cut segments for all trips: {end - start}")