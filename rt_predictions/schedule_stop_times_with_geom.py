"""
Collect pieces for compiling scheduled_stop_times
shape_geometry and stop_geometry.

Done in `rt_segment_speeds/A4_calculate_stop_delay`
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from segment_speed_utils import helpers, gtfs_schedule_wrangling
from segment_speed_utils.project_vars import (analysis_date, 
                                              PREDICTIONS_GCS)


def stop_times_with_shape_and_stop_geometry(
    analysis_date: str
) -> dg.GeoDataFrame:
    trips_with_geom = gtfs_schedule_wrangling.get_trips_with_geom(
        analysis_date, 
        trip_cols = ["feed_key", "name", "trip_id", 
                     "shape_id", "shape_array_key"]
    )

    stop_times = helpers.import_scheduled_stop_times(
        analysis_date, 
        columns = [
            "feed_key", "trip_id", 
            "stop_id", "stop_sequence",
        ]
    )

    scheduled_stop_times = gtfs_schedule_wrangling.merge_shapes_to_stop_times(
        trips_with_geom, stop_times) 

    stops = helpers.import_scheduled_stops(
            analysis_date, 
            columns = ["feed_key", "stop_id", "stop_name", "geometry"],
        )

    stop_times_with_geom = gtfs_schedule_wrangling.attach_stop_geometry(
        scheduled_stop_times,
        stops,
    )

    stop_times_with_geom = stop_times_with_geom.rename(
        columns = {"geometry_x": "shape_geometry", 
                   "geometry_y": "stop_geometry"
                  }).set_geometry("stop_geometry")
    
    stop_times_with_geom = stop_times_with_geom.repartition(
        partition_size="75MB")
    
    return stop_times_with_geom


if __name__ == "__main__":

    gdf = stop_times_with_shape_and_stop_geometry(analysis_date)
    gdf.to_parquet(
        f"{PREDICTIONS_GCS}scheduled_stop_times_with_geom_{analysis_date}")