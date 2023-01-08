"""
Skip the linear reference until sjoin to segments is done.
If linear reference is to derive distance elapsed between
two timestamps, then can we reference it against the segment?

We only need the enter/exit timestamps, rather than between each
point within a segment.

But, what to do with a segment that only has 1 point? 
It would still need a calculation derived, as an average, across the 
previous segment? or the post segment? But at that point, it 
can take the average for speed_mph...may not necessarily need
a shape_meters calculation?

https://stackoverflow.com/questions/24415806/coordinates-of-the-closest-points-of-two-geometries-in-shapely

https://github.com/dask/dask/issues/8042
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

#from shared_utils import geography_utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"
analysis_date = "2022-10-12"

fs = gcsfs.GCSFileSystem()

def get_scheduled_trips(analysis_date: str) -> dd.DataFrame:
    """
    Get scheduled trips info (all operators) for single day, 
    and keep subset of columns.
    """
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    keep_cols = ["calitp_itp_id", 
                 "trip_id", "shape_id",
                 "route_id", "direction_id"
                ] 
    
    trips2 = (trips[keep_cols]
              .reset_index(drop=True)
             )
    
    return trips2.compute()

              
def merge_scheduled_trips_with_segment_crosswalk(
    analysis_date: str) -> dd.DataFrame:
    """
    Merge scheduled trips with segments crosswalk to get 
    the route_dir_identifier for each trip.
    
    The route_dir_identifier is a more aggregated grouping 
    over which vehicle positions should be spatially joined to segments.
    """
    trips = get_scheduled_trips(analysis_date)
    
    segments_crosswalk = pd.read_parquet(
        f"{DASK_TEST}segments_route_direction_crosswalk.parquet")
               
    trips_with_route_dir_identifier = dd.merge(
        trips, 
        segments_crosswalk,
        on = ["calitp_itp_id", "route_id", "direction_id"],
        how = "inner",
    )
              
    return trips_with_route_dir_identifier
      
    
def get_unique_operators_route_directions(analysis_date: str) -> pd.DataFrame:
    """
    Import vehicle positions and merge onto scheduled trips and segments
    crosswalk to get route_dir_identifiers.
    
    Use this to break up the loop with dask delayed, by operator and
    by route_direction.
    """
    vp = pd.read_parquet(
        f"{DASK_TEST}vp_{analysis_date}.parquet", 
        columns = ["calitp_itp_id", "trip_id"]
    ).drop_duplicates().reset_index(drop=True)
              
    scheduled_trips_route_dir = merge_scheduled_trips_with_segment_crosswalk(
        analysis_date)
              
    trips_with_route_dir = pd.merge(
        vp,
        scheduled_trips_route_dir,
        on = ["calitp_itp_id", "trip_id"],
        how = "inner"
    ).drop_duplicates()
    
    keep_cols2 = ["calitp_itp_id",
                  "route_dir_identifier", "trip_id"]
    
    return trips_with_route_dir[keep_cols2]              
     

@delayed(nout=2)
def import_vehicle_positions_and_segments(
    vp_crosswalk: pd.DataFrame,
    itp_id: int, analysis_date: str,
    buffer_size: int = 50
) -> tuple[dg.GeoDataFrame]:
    """
    Import vehicle positions, filter to operator.
    Import route segments, filter to operator.
    """
    vp = dg.read_parquet(
        f"{DASK_TEST}vp_{analysis_date}.parquet", 
        filters = [[("calitp_itp_id", "==", itp_id)]]
    ).to_crs("EPSG:3310")
    
    vp2 = vp.drop_duplicates().reset_index(drop=True)
    
    vp_with_route_dir = dd.merge(
        vp2,
        vp_crosswalk,
        on = ["calitp_itp_id", "trip_id"],
        how = "inner"
    ).reset_index(drop=True).repartition(npartitions=1)
    
    segments = (dg.read_parquet(
                f"{DASK_TEST}longest_shape_segments.parquet", 
                filters = [[("calitp_itp_id", "==", itp_id)]]
            ).drop(columns = "calitp_url_number")
            .drop_duplicates()
            .reset_index(drop=True)
    )
    
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
    
    drop_cols = ["geometry", "vehicle_id", "entity_id"]
    ddf = vp_to_seg2.drop(columns = drop_cols)
    
    return ddf
        

def concat_and_export(filename: str, filetype: str = "df"):
    """
    Read in a folder of partitioned parquets and export as 1 parquet.
    """
    if filetype == "df":
        ddf = dd.read_parquet(filename)
        ddf.compute().to_parquet(filename)
        # Remove the folder version, not the single parquet
        fs.rm(f"{filename}/", recursive=True)
        
    elif filetype == "gdf":
        ddf = dg.read_parquet(filename)
        
    
def compute_and_export(results: list):
    time0 = datetime.datetime.now()
    
    results2 = [compute(i)[0] for i in results]
    ddf = dd.multi.concat(results2, axis=0).reset_index(drop=True)
    # is this only grabbing the first of the many delayed objects in the list?
    #df = dd.compute(*results2)[0]

    itp_id = ddf.calitp_itp_id.unique().compute().iloc[0]
    
    ddf = ddf.repartition(partition_size = "25MB")
    ddf.to_parquet(
        f"{DASK_TEST}vp_sjoin/vp_segment_{itp_id}_{analysis_date}.parquet")
    concat_and_export(
        f"{DASK_TEST}vp_sjoin/vp_segment_{itp_id}_{analysis_date}.parquet")
        
    time1 = datetime.datetime.now()
    logger.info(f"exported {itp_id}: {time1-time0}")
              


if __name__ == "__main__":
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    analysis_date = "2022-10-12"
    
    logger.add("./logs/A3_sjoin_vp_segments.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    vp_df = get_unique_operators_route_directions(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"get unique operators to loop over: {time1 - start}")
    
    ITP_IDS = vp_df.calitp_itp_id.unique()
    
    for itp_id in ITP_IDS:
        start_id = datetime.datetime.now()
        
        operator_routes = (vp_df[vp_df.calitp_itp_id == itp_id]
                           .route_dir_identifier.unique())
        
        operator_vp_df = (vp_df[vp_df.calitp_itp_id == itp_id]
                          .reset_index(drop=True))
        
        vp, segments = import_vehicle_positions_and_segments(
            operator_vp_df, 
            itp_id, 
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
        compute_and_export(results)
        
        end_id = datetime.datetime.now()
        logger.info(f"{itp_id}: {end_id-start_id}")
        
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    #client.close()