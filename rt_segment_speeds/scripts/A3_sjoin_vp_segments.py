"""
Spatial join vehicle positions to route segments.

Use a loop + dask.delayed to do the spatial join by
route-direction. Otherwise, points can be attached to other
routes that also travel on the same road.

Note: persist seems to help when delayed object is computed, but
it might not be necessary, since we don't actually compute anything 
beyond saving the parquet out.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import gcsfs
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

import dask_utils
import A1_vehicle_positions as A1
from update_vars import SEGMENT_GCS, COMPILED_CACHED_VIEWS, analysis_date

fs = gcsfs.GCSFileSystem()

              
def merge_scheduled_trips_with_segment_crosswalk(
    analysis_date: str) -> pd.DataFrame:
    """
    Merge scheduled trips with segments crosswalk to get 
    the route_dir_identifier for each trip.
    
    The route_dir_identifier is a more aggregated grouping 
    over which vehicle positions should be spatially joined to segments.
    """
    trips = A1.get_scheduled_trips(analysis_date)
    
    segments_crosswalk = pd.read_parquet(
        f"{SEGMENT_GCS}segments_route_direction_crosswalk_{analysis_date}.parquet")
               
    trips_with_route_dir_identifier = dd.merge(
        trips, 
        segments_crosswalk,
        on = ["feed_key", "name", "route_id", "direction_id"],
        how = "inner",
    ).compute()
              
    return trips_with_route_dir_identifier
      
    
def get_unique_operators_route_directions(analysis_date: str) -> pd.DataFrame:
    """
    Import vehicle positions and merge onto scheduled trips and segments
    crosswalk to get route_dir_identifiers.
    
    Use this to break up the loop with dask delayed, by operator and
    by route_direction.
    """
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet", 
        columns = ["gtfs_dataset_key", "_gtfs_dataset_name", "trip_id"]
    ).drop_duplicates().reset_index(drop=True)
              
    scheduled_trips_route_dir = merge_scheduled_trips_with_segment_crosswalk(
        analysis_date)
              
    trips_with_route_dir = pd.merge(
        vp,
        scheduled_trips_route_dir,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    ).drop_duplicates()
    
    keep_cols2 = ["gtfs_dataset_key",
                  "route_dir_identifier", "trip_id"]
    
    return trips_with_route_dir[keep_cols2]              
     

@delayed(nout=2)
def import_vehicle_positions_and_segments(
    vp_crosswalk: pd.DataFrame,
    feed_key: int, analysis_date: str,
    buffer_size: int = 50
) -> tuple[dg.GeoDataFrame]:
    """
    Import vehicle positions, filter to operator.
    Import route segments, filter to operator.
    """
    vp = dg.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet", 
        filters = [[("gtfs_dataset_key", "==", feed_key)]]
    ).to_crs("EPSG:3310")
    
    vp2 = vp.drop_duplicates().reset_index(drop=True)
    
    # vp_crosswalk contains the trip_ids that are present for this operator
    # Do merge to get the route_dir_identifiers that remain
    vp_with_route_dir = dd.merge(
        vp2,
        vp_crosswalk,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    ).reset_index(drop=True).repartition(npartitions=1)
    
    operator_routes_present = (vp_with_route_dir.route_dir_identifier.unique()
                               .compute().tolist())
    
    # For the route_dir_identifiers present, subset segments
    # vp can only be spatially joined to segments for that route
    segments = dg.read_parquet(
                f"{SEGMENT_GCS}longest_shape_segments_{analysis_date}.parquet", 
                filters = [[("gtfs_dataset_key", "==", feed_key)]],
                columns = ["gtfs_dataset_key", 
                           "route_dir_identifier", "segment_sequence",
                          "geometry"]
    ).drop_duplicates().reset_index(drop=True)
    
    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments_buff = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    return vp_with_route_dir, segments_buff


@delayed
def sjoin_vehicle_positions_to_segments(
    vehicle_positions: dg.GeoDataFrame, 
    segments: dg.GeoDataFrame,
    route_identifier: int
) -> dd.DataFrame:
    """
    Spatial join vehicle positions for an operator
    to buffered route segments.
    
    Returns a dd.DataFrame. geometry seems to make the 
    compute extremely large. 
    Do more aggregation at segment-level before bringing 
    point geom back in for linear referencing.
    """    
    vehicle_positions_subset = (vehicle_positions[
        vehicle_positions.route_dir_identifier==route_identifier]
        .reset_index(drop=True))
        
    segments_subset = (segments[
        segments.route_dir_identifier==route_identifier]
       .reset_index(drop=True)
      )
    
    # Once we filter for the route_dir_identifier, don't need to include
    # it into segment_cols, otherwise, it'll show up as _x and _y
    segment_cols = ["segment_sequence"]    

    vp_to_seg = dg.sjoin(
        vehicle_positions_subset,
        segments_subset[
            segment_cols + ["geometry"]],
        how = "inner",
        predicate = "within"
    ).drop(columns = "index_right").drop_duplicates().reset_index(drop=True)
    
    
    # Drop geometry and return a df...eventually,
    # can attach point geom back on, after enter/exit points are kept
    # geometry seems to be a big issue in the compute
    vp_to_seg2 = vp_to_seg.assign(
        lon = vp_to_seg.geometry.x,
        lat = vp_to_seg.geometry.y,
    )
    
    ddf = vp_to_seg2.drop(columns = ["geometry"])
    
    return ddf
       
    
if __name__ == "__main__":
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
        
    logger.add("../logs/A3_sjoin_vp_segments.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    vp_df = get_unique_operators_route_directions(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"get unique operators to loop over: {time1 - start}")
    
    RT_OPERATORS = sorted(vp_df.gtfs_dataset_key.unique().tolist())
    
    for rt_dataset_key in RT_OPERATORS:
        start_id = datetime.datetime.now()
        
        operator_vp_df = (vp_df[vp_df.gtfs_dataset_key == rt_dataset_key]
                          .reset_index(drop=True))
        
        operator_routes = operator_vp_df.route_dir_identifier.unique()
        
        vp, segments = import_vehicle_positions_and_segments(
            operator_vp_df, 
            rt_dataset_key, 
            analysis_date, buffer_size=50).persist()
        
        results = []
        
        for route_identifier in operator_routes:
            
            vp_to_segment = sjoin_vehicle_positions_to_segments(
                vp,
                segments, 
                route_identifier
            ).persist()
            
            results.append(vp_to_segment)
            
        # Compute the list of delayed objects
        #compute_and_export(results)
        dask_utils.compute_and_export(
            results,
            gcs_folder = f"{SEGMENT_GCS}vp_sjoin/",
            file_name = f"vp_segment_{rt_dataset_key}_{analysis_date}.parquet",
            export_single_parquet = True
        )
        
        end_id = datetime.datetime.now()
        logger.info(f"{rt_dataset_key}: {end_id-start_id}")
        
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    #client.close()