"""
Create a proxy stop_times table for road segments.
The "stops" along a road segment are where the segment 
starts and ends.
These are the points we want to find the nearest vp
against, and see the interpolated arrival time at that position.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from calitp_data_analysis import utils
from segment_speed_utils import helpers
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS, SHARED_GCS
from segment_speed_utils.project_vars import PROJECT_CRS

def road_endpoints_as_stops(
    analysis_date: str,
    road_segments_path: str,    
) -> gpd.GeoDataFrame:
    road_seg_cols = [*GTFS_DATA_DICT.road_segments.road_segment_cols]
    
    shapes_to_roads = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}roads_staging/"
        f"shape_road_crosswalk_{analysis_date}.parquet",
    )    

    subset_linearids = shapes_to_roads.linearid.unique()
    
    # For the subset of roads we're interested in,
    # grab the origin and destination
    # and concatenate those to be road stop_times.
    road_origins = delayed(gpd.read_parquet)(
        f"{SHARED_GCS}{road_segments_path}.parquet",
        columns = road_seg_cols + ["origin", "primary_direction"],
        filters = [[("linearid", "in", subset_linearids)]]
    )

    road_destinations = delayed(gpd.read_parquet)(
        f"{SHARED_GCS}{road_segments_path}.parquet",
        columns = road_seg_cols + ["destination", "primary_direction"],
        filters = [[("linearid", "in", subset_linearids)]]
    )

    road_stops = delayed(pd.concat)([
        road_origins.rename(
            columns = {"origin": "geometry"}
        ).assign(stop_type = 0),
        road_destinations.rename(
            columns = {"destination": "geometry"}
        ).assign(stop_type = 1)
        ], axis=0, ignore_index=True
    ).astype({"stop_type": "int8"})
        
    # Set as gdf
    road_stops = delayed(gpd.GeoDataFrame)(
        road_stops, 
        geometry="geometry", 
        crs=PROJECT_CRS
    )
    
    # Add shapes to road origins/destinations
    road_stops_with_shape = delayed(pd.merge)(
        road_stops,
        shapes_to_roads,
        on = road_seg_cols,
        how = "inner"
    )
    
    road_stops_with_shape = compute(road_stops_with_shape)[0]
    
    return road_stops_with_shape


def proxy_road_stops(
    #road_stops: gpd.GeoDataFrame,
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Input road stops for each shape. Merge in trips-to-shape crosswalk
    so we can expand those rows so each trip is represented.
    """
    road_stops = gpd.read_parquet(
        f"{SEGMENT_GCS}stop_time_expansion/road_stops_{analysis_date}.parquet"
    )

    subset_shapes = road_stops.shape_array_key.unique()
    
    rt_trips = helpers.import_unique_vp_trips(analysis_date)
    
    trips_in_crosswalk = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "trip_instance_key", "shape_array_key"],
        filters = [[("shape_array_key", "in", subset_shapes), 
                    ("trip_instance_key", "in", rt_trips)]],
        get_pandas = True
    )
    
    # Now expand road_stops, which were for each shape
    # to have rows holding each trip_instance_key for that shape
    # At this point, we have only used scheduled trips / shapes
    # and not vp_trips
    road_stops_all_trips = pd.merge(
        road_stops,
        trips_in_crosswalk,
        on = "shape_array_key",
        how = "inner"
    )
    
    # Avoid kernel crashing, we'll export as dask ddf
    road_stops_all_trips = dd.from_pandas(
        road_stops_all_trips, npartitions=100)
    
    return road_stops_all_trips


if __name__ == "__main__":
    
    LOG_FILE = "../logs/stop_time_expansion.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    from segment_speed_utils.project_vars import analysis_date
    
    ROAD_SEGMENTS = GTFS_DATA_DICT.shared_data.road_segments_onekm
    EXPORT = GTFS_DATA_DICT.road_segments.proxy_stop_times
    
    start = datetime.datetime.now()
    
    road_stops = road_endpoints_as_stops(analysis_date, ROAD_SEGMENTS)
    
    utils.geoparquet_gcs_export(
        road_stops,
        SEGMENT_GCS,
        f"stop_time_expansion/road_stops_{analysis_date}"
    )    
    
    print("road stops exported")
    
    road_stop_times = proxy_road_stops(analysis_date)
     
    road_stop_times.to_parquet(
        f"{SEGMENT_GCS}{EXPORT}_{analysis_date}",
        partition_on = "schedule_gtfs_dataset_key",
        overwrite=True
    )
    
    end = datetime.datetime.now()
    logger.info(f"road stop time creation {analysis_date}: {end - start}")
    
    