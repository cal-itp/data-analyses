"""
Filter out unusable trips using RT trip diagnostics.

Keep the enter / exit points for each segment.
"""
import dask.dataframe as dd
import datetime
import numpy as np
import pandas as pd
import sys

from loguru import logger

import loop_utils
from segment_speed_utils import helpers, segment_calcs
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)

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
    
def usable_no_loop_inlining_trips(
    vp: dd.DataFrame
) -> dd.DataFrame:   
    """
    From trips with enough RT vehicle positions,
    exclude the trips that have looping/inlining.
    """
    vp_trips = vp[
        ["gtfs_dataset_key", "feed_key", 
         "trip_id"]].drop_duplicates()
    
    loop_trips = loop_utils.grab_loop_trips(analysis_date)
    
    trips_no_loops = dd.merge(
        vp_trips,
        loop_trips,
        on = ["feed_key", "trip_id"],
        how = "left",
        indicator=True
    ).query('_merge=="left_only"').drop(columns = ["_merge", "feed_key"])
    
    vp_simple_trips = dd.merge(
        vp,
        trips_no_loops,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    )
    
    return vp_simple_trips


def pare_down_vp_by_segment(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    Pare down vehicle positions that have been joined to segments
    to keep the enter / exit timestamps.
    Also, exclude any bad batches of trips.
    """
    INPUT_FILE_PREFIX = dict_inputs["stage2"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]
    EXPORT_FILE = dict_inputs["stage3"]

    # https://docs.dask.org/en/stable/delayed-collections.html
    vp_joined_to_segments = helpers.import_vehicle_positions(
        f"{SEGMENT_GCS}vp_sjoin/",
        f"{INPUT_FILE_PREFIX}_{analysis_date}",
        file_type = "df",
        partitioned=True
    )
    
    valid_vp_joined_to_segments = usable_no_loop_inlining_trips(
        vp_joined_to_segments
    )
    
    logger.info("filter out to only valid trips")
        
    vp_pared = segment_calcs.keep_min_max_timestamps_by_segment(
        valid_vp_joined_to_segments, 
        segment_identifier_cols = SEGMENT_IDENTIFIER_COLS,
        timestamp_col = TIMESTAMP_COL
    ).repartition(partition_size="85MB")
    
    vp_pared.to_parquet(f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}")
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/valid_vehicle_positions.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
   
    time1 = datetime.datetime.now()
    pare_down_vp_by_segment(
        analysis_date,
        dict_inputs = ROUTE_SEG_DICT
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"pare down vp by route segments {time2 - time1}")
    
    pare_down_vp_by_segment(
        analysis_date,
        dict_inputs = STOP_SEG_DICT
    )
    
    time3 = datetime.datetime.now()
    logger.info(f"pare down vp by stop segments {time3 - time2}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")