import dask_geopandas as dg
import dask.dataframe as dd
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from loguru import logger

from shared_utils import rt_dates
from segment_speed_utils import helpers, neighbor
from update_vars import SEGMENT_GCS, SHARED_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import PROJECT_CRS


def stop_times_for_shape_segments(
    analysis_date: str,
    dict_inputs: dict
) -> gpd.GeoDataFrame:
    """
    This is the stop times table using only 1 shape for each 
    route-direction. Every trip belong to that shape
    will be cut along the same stops.
    This allows us to aggregate segments across trips because each 
    segment has the same stop_id1 and stop_id2.
    """
    SEGMENT_FILE = dict_inputs["segments_file"]
    
    rt_trips = helpers.import_unique_vp_trips(analysis_date)

    shape_stop_combinations = pd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}_{analysis_date}.parquet",
        columns = ["trip_instance_key",
                   "stop_id1", "stop_pair",
                   "st_trip_instance_key"],
        filters = [[
            #("schedule_gtfs_dataset_key", "==", "7cc0cb1871dfd558f11a2885c145d144"),
            ("trip_instance_key", "in", rt_trips)
        ]]
    ).rename(columns = {"stop_id1": "stop_id"})
    
    subset_trips = shape_stop_combinations.st_trip_instance_key.unique()
    
    stops_to_use = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key",
                   "stop_sequence", "stop_id", "stop_pair",
                   "stop_primary_direction", "geometry"],
        filters = [[("trip_instance_key", "in", subset_trips)]],
        get_pandas = True,
        with_direction = True
    ).rename(columns = {"trip_instance_key": "st_trip_instance_key"})
    
    stop_times = pd.merge(
        stops_to_use,
        shape_stop_combinations,
        on = ["st_trip_instance_key", "stop_id", "stop_pair"],
        how = "inner"
    ).drop(
        columns = "st_trip_instance_key"
    ).drop_duplicates().reset_index(drop=True)
        
    return stop_times


def new_nearest_neighbor_for_stop(
    analysis_date: str,
    segment_type = segment_type,
    config_path = GTFS_DATA_DICT
):
    """
    """
    start = datetime.datetime.now()
    
    dict_inputs = config_path[segment_type]
    trip_stop_cols = [*dict_inputs["trip_stop_cols"]]
    EXPORT_FILE = dict_inputs["stage2c"]
    
    stop_times = stop_times_for_shape_segments(
        analysis_date, 
        dict_inputs
    )
    
    gdf = neighbor.new_merge_stop_vp_for_nearest_neighbor(stop_times, analysis_date)
    
    vp_before, vp_after = np.vectorize(
        neighbor.new_subset_arrays_to_valid_directions
    )(
        gdf.vp_primary_direction, 
        gdf.vp_geometry, 
        gdf.vp_idx,
        gdf.stop_geometry,
        gdf.stop_primary_direction,
        gdf.shape_geometry,
        gdf.stop_meters
    )

    gdf2 = gdf.assign(
        before_vp_idx = vp_before,
        after_vp_idx = vp_after,
    )[trip_stop_cols + [
        "shape_array_key", "stop_meters", "before_vp_idx", "after_vp_idx"]
    ]
        
    del gdf, stop_times
    
    gdf2.to_parquet(f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet")
    
    end = datetime.datetime.now()
    logger.info(f"nearest neighbor for {segment_type} "
                f"{analysis_date}: {end - start}")    
        
    return 

    
'''
if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    from dask import delayed, compute
    
    delayed_dfs = [
        delayed(new_nearest_neighbor_for_stop)(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        ) for analysis_date in analysis_date_list
    ]

    [compute(i)[0] for i in delayed_dfs]
'''