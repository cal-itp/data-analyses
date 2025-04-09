import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
import sys

from loguru import logger

from calitp_data_analysis import utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import rt_dates


def set_dataset(analysis_date: str):
    
    VP_CONDENSED = GTFS_DATA_DICT.speeds_tables.vp_condensed_line

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
    
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}{VP_CONDENSED}_{analysis_date}.parquet",
        columns = ["trip_instance_key", "location_timestamp_local", "geometry"],
        filters = [[("trip_instance_key", "in", trips.trip_instance_key.tolist())]]
    ).to_crs(PROJECT_CRS).rename(
        columns = {"geometry": "vp_geometry"}
    ).set_geometry("vp_geometry")

    gdf = pd.merge(
        vp,
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "trip_instance_key",
        how = "inner"
    )
        
    return gdf


def resample_vp(gdf: gpd.GeoDataFrame):
    # Get a version where we take the array of timestamps, convert it to seconds,
    # resample it and get it at a higher frequency (5-10 seconds), and get the distance against shape
    # that's our vp_meters
    # for vp paths that follow shapes that are simple, this should work ok
    new_timestamps_series = []
    vp_meters_series = []
    vp_geom_series = []

    for row in gdf.itertuples():
        vp_points = np.asarray(getattr(row, "vp_geometry").coords)
        vp_meters = np.asarray(
            [getattr(row, "shape_geometry").project(shapely.Point(p)) 
             for p in vp_points])
    
        timestamps = np.asarray(getattr(row, "location_timestamp_local").astype("datetime64[s]").astype("float64"))

        # Resampled seconds
        timestamps_new = np.arange(min(timestamps), max(timestamps), step=5)
        new_timestamps_series.append(timestamps_new)
    
        vp_meters_new = np.interp(timestamps_new, timestamps, vp_meters)
        vp_meters_series.append(vp_meters_new)

        new_vp_positions = shapely.LineString(
            [getattr(row, "vp_geometry").interpolate(d) for d in vp_meters_new]) 
        new_vp_points = [shapely.Point(p) for p in new_vp_positions.coords]
        vp_geom_series.append(new_vp_positions)
    
    
    gdf2 = gdf.assign(
        resampled_times = new_timestamps_series,
        interpolated_distances = vp_meters_series,
        geometry = gpd.GeoSeries(vp_geom_series, crs = PROJECT_CRS),
    ).drop(
        columns = ["vp_geometry", "shape_geometry"]
    ).set_geometry("geometry")
    
    return gdf2


if __name__ == "__main__":

    LOG_FILE = "../logs/resampling.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    start = datetime.datetime.now()
    
    analysis_date = rt_dates.DATES["oct2024"]
    
    gdf = set_dataset(analysis_date)

    results = resample_vp(gdf)

    utils.geoparquet_gcs_export(
        results,
        SEGMENT_GCS,
        f"vp_condensed/vp_resampled_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    logger.info(f"{analysis_date} resampling: {end - start}")

    # when we use series, arrays + keep 1 geometry column
    # 2024-10-16 resampling: 0:55:21.954823 when we use itertuples + keep 1 geometry column  