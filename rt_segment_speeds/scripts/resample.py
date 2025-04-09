import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
import sys

from loguru import logger

from calitp_data_analysis import utils
from numba import jit

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


def project_vp_on_shape(gdf: gpd.GeoDataFrame):
    # Get a version where we take the array of timestamps, convert it to seconds,
    # resample it and get it at a higher frequency (5-10 seconds), and get the distance against shape
    # that's our vp_meters
    # for vp paths that follow shapes that are simple, this should work ok
    vp_meters_series = []

    for row in gdf.itertuples():
        vp_points = np.asarray(getattr(row, "vp_geometry").coords)
        vp_meters = np.asarray(
            [getattr(row, "shape_geometry").project(shapely.Point(p)) 
             for p in vp_points])
        
        vp_meters_series.append(vp_meters)
        

    gdf2 = gdf.assign(
        interpolated_distances = vp_meters_series,
        timestamps = gdf.apply(
            lambda x: 
            np.asarray(x.location_timestamp_local).astype("datetime64[s]").astype("float64"), 
            axis=1)
    )[["trip_instance_key", "vp_geometry", "interpolated_distances", "timestamps"]]
    
    return gdf2


@jit(nopython=True)
def resample_vp_timestamps(timestamp_array, seconds_step: int):
    return np.arange(min(timestamp_array), max(timestamp_array), step=seconds_step)


@jit(nopython=True)
def interpolate_step(timestamp_resampled, timestamp_orig, vp_meters):
    return np.interp(timestamp_resampled, timestamp_orig, vp_meters)


def resample_with_numba(gdf):
    """
    Split apart steps that are not shapely and use numba on it.
    """
    gdf2 = project_vp_on_shape(gdf)
    vp_meters_series = gdf2.interpolated_distances
    timestamps_series = gdf2.timestamps
    resampled_timestamps = [resample_vp_timestamps(t, 5) for t in timestamps_series]
    vp_meters_new = [interpolate_step(timestamps_new, timestamps, vp_meters) 
                 for timestamps_new, timestamps, vp_meters 
                 in zip(resampled_timestamps, timestamps_series, vp_meters_series)]


    gdf2 = gdf2.assign(
        resampled_timestamps = resampled_timestamps,
        new_distances = vp_meters_new
    )
    
    vp_geom_series = []
    for row in gdf2.itertuples():
        vp_meters_new = getattr(row, "new_distances")
        new_vp_positions = shapely.LineString(
            [getattr(row, "vp_geometry").interpolate(d) for d in vp_meters_new]) 
        vp_geom_series.append(new_vp_positions)       

    gdf3 = gdf2.assign(
        geometry = gpd.GeoSeries(vp_geom_series, crs = PROJECT_CRS),
    ).drop(columns = "vp_geometry").set_geometry("geometry")
    
    return gdf3



if __name__ == "__main__":

    
    LOG_FILE = "../logs/resampling.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    start = datetime.datetime.now()
    
    analysis_date = rt_dates.DATES["oct2024"]
    
    gdf = set_dataset(analysis_date)

    results = resample_with_numba(gdf)
  
    utils.geoparquet_gcs_export(
        results,
        SEGMENT_GCS,
        f"vp_condensed/vp_resampled2_{analysis_date}"
    )

    
    end = datetime.datetime.now()
    logger.info(f"{analysis_date} resampling: {end - start}")

    # 2024-10-16 resampling: 0:55:21.954823 when we use itertuples + keep 1 geometry column  
    # 2024-10-16 resampling: 0:39:07.934167 use numba 
