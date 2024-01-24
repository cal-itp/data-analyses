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
    shape_geometry: shapely.Geometry,
    timestamp_arr: np.ndarray,
) -> float:
    points = [shapely.Point(p) for p in vp_coords_trio.coords]
    vp_projected = np.asarray([shape_geometry.project(p) for p in points])

    stop_position = shape_geometry.project(stop_geometry)
    xp = vp_projected
    yp = timestamp_arr.astype("datetime64[s]").astype("float64")

    interpolated_arrival = np.interp(stop_position, xp, yp)
    
    return interpolated_arrival

def main(
    analysis_date: str,
    dict_inputs: dict
):
    
    NEAREST_VP = f"{dict_inputs['nearest_shape_segments']}_{analysis_date}"
    STOP_ARRIVALS_FILE = f"{dict_inputs['stage3']}_{analysis_date}"
    
    start = datetime.datetime.now()
    
    df = gpd.read_parquet(
        f"{SEGMENT_GCS}nearest/{NEAREST_VP}.parquet",
    )

    df = df.assign(
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

    stop_arrival_series = []

    for row in gdf.itertuples():
        stop_geom = getattr(row, "stop_geometry")
        vp_coords = getattr(row, "vp_coords_trio")
        shape_geom = getattr(row, "shape_geometry")


        interpolated_arrival = project_points_onto_shape(
            getattr(row, "stop_geometry"),
            getattr(row, "vp_coords_trio"),
            getattr(row, "shape_geometry"),
            getattr(row, "location_timestamp_local_trio")
        )

        stop_arrival_series.append(interpolated_arrival)

    results = gdf.assign(
        arrival_time = stop_arrival_series,
    ).astype({"arrival_time": "datetime64[s]"})[
        ["trip_instance_key", "shape_array_key",
         "stop_sequence", "stop_id",
         "arrival_time"
    ]]

    del gdf

    results.to_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}_{analysis_date}.parquet"
    )

    end = datetime.datetime.now()
    print(f"execution time: {end - start}")


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/interpolate_stop_arrival.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    for analysis_date in analysis_date_list:
        logger.info(f"Analysis date: {analysis_date}")
        
        main(analysis_date, STOP_SEG_DICT, "stop_segments")

        