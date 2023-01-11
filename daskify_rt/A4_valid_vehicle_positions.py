"""
Filter out unusable trips using RT trip diagnostics.

Keep the enter / exit points for each segment.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import gcsfs
import numpy as np
import pandas as pd
import sys
import warnings

from dask import delayed
from loguru import logger
from shapely.errors import ShapelyDeprecationWarning
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

import dask_utils

"""
References

Why call compute twice on dask delayed?
https://stackoverflow.com/questions/56944723/why-sometimes-do-i-have-to-call-compute-twice-on-dask-delayed-functions

Parallelize dask aggregation
https://stackoverflow.com/questions/62352617/parallelizing-a-dask-aggregation

Dask delayed stuff
https://docs.dask.org/en/latest/delayed.htmls
https://tutorial.dask.org/03_dask.delayed.html
https://stackoverflow.com/questions/71736681/possible-overhead-on-dask-computation-over-list-of-delayed-objects
https://docs.dask.org/en/stable/delayed-collections.html
https://distributed.dask.org/en/latest/manage-computation.html
https://docs.dask.org/en/latest/delayed-best-practices.html

Map partitions has trouble computing result.
Just use partitioned df and don't use `ddf.map_partitions`.
"""

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"

analysis_date = "2022-10-12"
fs = gcsfs.GCSFileSystem()
                    

def operators_with_data(gcs_folder: str = f"{DASK_TEST}vp_sjoin/") -> list:
    """
    Return a list of operators with RT vehicle positions 
    spatially joined to route segments.
    """
    all_files_in_folder = fs.ls(gcs_folder)
    
    files = [i for i in all_files_in_folder if "vp_segment_" in i]
    
    ITP_IDS = [int(i.split('vp_segment_')[1]
               .split(f'_{analysis_date}')[0]) 
           for i in files]
    
    return ITP_IDS
    

@delayed    
def import_vehicle_positions(itp_id: int) -> dd.DataFrame:
    """
    Import vehicle positions spatially joined to segments for 
    each operator.
    """
    vp_segments = dd.read_parquet(
            f"{DASK_TEST}vp_sjoin/vp_segment_{itp_id}_{analysis_date}.parquet")

    vp_segments = vp_segments.repartition(partition_size = "85MB")
    
    return vp_segments


@delayed
def exclude_unusable_trips(vp_df: dd.DataFrame, 
                           valid_trip_ids: list) -> dd.DataFrame:
    """
    PLACEHOLDER FUNCTION
    Figure out trip-level diagnostics first.
    Supply a list of valid trips or trips to exclude?
    """
    return vp_df[vp_df.trip_id.isin(trips_list)].reset_index(drop=True)
    
    
# https://stackoverflow.com/questions/58145700/using-groupby-to-store-value-counts-in-new-column-in-dask-dataframe
# https://github.com/dask/dask/pull/5327
@delayed
def keep_min_max_timestamps_by_segment(
    vp_to_seg: dd.DataFrame) -> dd.DataFrame:
    """
    For each segment-trip combination, throw away excess points, just 
    keep the enter/exit points for the segment.
    """
    segment_cols = ["route_dir_identifier", "segment_sequence"]
    segment_trip_cols = ["calitp_itp_id", "trip_id"] + segment_cols
    
    # https://stackoverflow.com/questions/52552066/dask-compute-gives-attributeerror-series-object-has-no-attribute-encode    
    # comment out .compute() and just .reset_index()
    enter = (vp_to_seg.groupby(segment_trip_cols)
             .vehicle_timestamp.min()
             #.compute()
             .reset_index()
            )

    exit = (vp_to_seg.groupby(segment_trip_cols)
            .vehicle_timestamp.max()
            #.compute()
            .reset_index()
           )
    
    enter_exit = dd.multi.concat([enter, exit], axis=0)
    
    # Merge back in with original to only keep the min/max timestamps
    # dask can't sort by multiple columns to drop
    enter_exit_full_info = dd.merge(
        vp_to_seg,
        enter_exit,
        on = segment_trip_cols + ["vehicle_timestamp"],
        how = "inner"
    ).reset_index(drop=True)
        
    return enter_exit_full_info


if __name__ == "__main__": 
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/A4_valid_vehicle_positions.log", 
               retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    ITP_IDS = operators_with_data(f"{DASK_TEST}vp_sjoin/")  
    
    results = []
    
    for itp_id in ITP_IDS:
        logger.info(f"start {itp_id}")
        
        start_id = datetime.datetime.now()

        # https://docs.dask.org/en/stable/delayed-collections.html
        operator_vp_segments = import_vehicle_positions(itp_id)
        
        time1 = datetime.datetime.now()
        logger.info(f"imported data: {time1 - start_id}")
        
        # filter to usable trips
        # pass valid_operator_vp down 
        # valid_operator_vp = exclude_unusable_trips(
        # operator_vp_segments, trips_list)
        #logger.info(f"filter out to only valid trips: {}")
        
        vp_pared = keep_min_max_timestamps_by_segment(
            operator_vp_segments)
        
        results.append(vp_pared)
        
        time2 = datetime.datetime.now()
        logger.info(f"keep enter/exit points by segment-trip: {time2 - time1}")
        
        end_id = datetime.datetime.now()
        logger.info(f"ITP ID: {itp_id}: {end_id-start_id}")

    
    time3 = datetime.datetime.now()
    logger.info(f"start compute and export of results")
    
    # Unpack delayed results
    dask_utils.compute_and_export(
        results, 
        gcs_folder = f"{DASK_TEST}",
        file_name = f"vp_pared_{analysis_date}",
        export_single_parquet=False
    )
    
    time4 = datetime.datetime.now()
    logger.info(f"exported all vp pared: {time4 - time3}")

    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    #client.close()
        