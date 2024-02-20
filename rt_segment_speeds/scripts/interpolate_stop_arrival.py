"""
Interpolate stop arrival.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from loguru import logger

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, PROJECT_CRS, 
                                              CONFIG_PATH
                                             )

def project_points_onto_shape(
    stop_geometry: shapely.Point,
    vp_coords_trio: shapely.LineString,
    shape_geometry: shapely.LineString,
    timestamp_arr: np.ndarray,
) -> tuple[float]:
    """
    Project the points in the vp trio against the shape geometry
    and the stop position onto the shape geometry.
    Use np.interp to find interpolated arrival time
    """
    stop_position = vp_coords_trio.project(stop_geometry)
    stop_meters = shape_geometry.project(stop_geometry)
    
    points = [shapely.Point(p) for p in vp_coords_trio.coords]
    xp = np.asarray([vp_coords_trio.project(p) for p in points])

    yp = timestamp_arr.astype("datetime64[s]").astype("float64")

    interpolated_arrival = np.interp(stop_position, xp, yp)
        
    return stop_meters, interpolated_arrival


def interpolate_stop_arrivals(
    analysis_date: str,
    dict_inputs: dict
):
    """
    Find the interpolated stop arrival based on where 
    stop is relative to the trio of vehicle positions.
    """
    NEAREST_VP = f"{dict_inputs['stage2']}_{analysis_date}"
    STOP_ARRIVALS_FILE = f"{dict_inputs['stage3']}_{analysis_date}"
    
    start = datetime.datetime.now()
    
    # Import nearest vp file, set to EPSG:3310 
    # to merge against scheduled shapes
    df = gpd.read_parquet(
        f"{SEGMENT_GCS}{NEAREST_VP}.parquet",
    )

    df = df.assign(
        stop_geometry = df.stop_geometry.to_crs(PROJECT_CRS),
        vp_coords_trio = df.vp_coords_trio.to_crs(PROJECT_CRS)
    )

    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS
    )

    gdf = pd.merge(
        df,
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    )

    del df, shapes

    stop_meters_series = []
    stop_arrival_series = []
    
    for row in gdf.itertuples():
        
        stop_meters, interpolated_arrival = project_points_onto_shape(
            getattr(row, "stop_geometry"),
            getattr(row, "vp_coords_trio"),
            getattr(row, "shape_geometry"),
            getattr(row, "location_timestamp_local_trio")
        )
        
        stop_meters_series.append(stop_meters)
        stop_arrival_series.append(interpolated_arrival)

    results = gdf.assign(
        stop_meters = stop_meters_series,
        arrival_time = stop_arrival_series,
    ).astype({"arrival_time": "datetime64[s]"})[
        ["trip_instance_key", "shape_array_key",
         "stop_sequence", "stop_id", 
         "stop_meters",
         "arrival_time"
    ]]

    del gdf

    results.to_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}.parquet"
    )

    end = datetime.datetime.now()
    logger.info(f"get stop arrivals {analysis_date}: {end - start}")


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/interpolate_stop_arrival.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    RT_STOP_TIMES_DICT = helpers.get_parameters(CONFIG_PATH, "rt_stop_times")

    for analysis_date in analysis_date_list:        
        interpolate_stop_arrivals(analysis_date, STOP_SEG_DICT)
        #interpolate_stop_arrivals(analysis_date, RT_STOP_TIMES_DICT)

        