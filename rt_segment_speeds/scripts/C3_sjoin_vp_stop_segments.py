import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed
from loguru import logger

from shared_utils import dask_utils, utils
from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, analysis_date

@delayed(nout=2)
def import_vehicle_positions_and_segments(
    rt_dataset_key: int, 
    analysis_date: str,
    buffer_size: int = 50
) -> tuple[dg.GeoDataFrame]:
    """
    Import vehicle positions, filter to operator.
    Import route segments, filter to operator.
    """
    # For the route_dir_identifiers present, subset segments
    # vp can only be spatially joined to segments for that route    
    segments = helpers.import_segments(
        SEGMENT_GCS, 
        f"stop_segments_{analysis_date}",
        filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]], 
        columns = ["gtfs_dataset_key", "stop_sequence", 
                   "shape_array_key", "geometry"]
    )
    
    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments_buff = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    
    vp = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        f"vp_{analysis_date}",
        file_type = "gdf",
        filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]]
    )
    
    # Get crosswalk
    trip_grouping_cols = ["feed_key", "trip_id", 
                           "shape_array_key"]
    crosswalk = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, trip_grouping_cols
    )
    
    group_cols = ["gtfs_dataset_key", "shape_array_key"]
    segments_new_grouping = segments[group_cols].drop_duplicates()
    
    # vp_crosswalk contains the trip_ids that are present for this operator
    # Do merge to get the route_dir_identifiers that remain
    vp_with_shape_array = dd.merge(
        vp2,
        crosswalk,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    ).merge(
        segments_new_grouping,
        on = group_cols,
        how = "inner"
    ).reset_index(drop=True).repartition(npartitions=1)
    
    return vp_with_shape_array, segments_buff


if __name__ == "__main__":
    logger.add("../logs/C3_sjoin_vp_stop_segments.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    vp_df = dg.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet", 
        columns = ["gtfs_dataset_key"]
    ).drop_duplicates().compute()
    
    time1 = datetime.datetime.now()
    logger.info(f"get unique operators to loop over: {time1 - start}")
    
    RT_OPERATORS = sorted(vp_df.gtfs_dataset_key.unique().tolist())
    
    for rt_dataset_key in RT_OPERATORS:
        start_id = datetime.datetime.now()
        
        vp, segments = import_vehicle_positions_and_segments(
            rt_dataset_key, 
            analysis_date, 
            buffer_size=50
        ).persist()
        
        operator_routes = vp.shape_array_key.unique().compute()

        results = []
        
        for shape_key in operator_routes:
            
            vp_to_segment = delayed(
                helpers.sjoin_vehicle_positions_to_segments)(
                vp,
                segments, 
                route_tuple = ("shape_array_key", shape_key),
                segment_identifier_cols = ["stop_sequence"]
            ).persist()
            
            results.append(vp_to_segment)
            
        # Compute the list of delayed objects
        if len(results) > 0:
            dask_utils.compute_and_export(
                results,
                gcs_folder = f"{SEGMENT_GCS}vp_sjoin/",
                file_name = (f"vp_stop_segment_"
                             f"{rt_dataset_key}_{analysis_date}.parquet"),
                export_single_parquet = True
            )
        
        end_id = datetime.datetime.now()
        logger.info(f"{rt_dataset_key}: {end_id-start_id}")
        
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
 
 
