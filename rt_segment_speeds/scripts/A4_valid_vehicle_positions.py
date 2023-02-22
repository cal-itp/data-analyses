"""
Filter out unusable trips using RT trip diagnostics.

Keep the enter / exit points for each segment.
"""
import dask.dataframe as dd
import datetime
import pandas as pd
import sys

from dask import delayed
from loguru import logger

from shared_utils import dask_utils
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

def pare_down_vp_to_valid_trips(
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
    vp_joined_to_segments = delayed(
        helpers.import_vehicle_positions)(
        f"{SEGMENT_GCS}vp_sjoin/",
        f"{INPUT_FILE_PREFIX}_{analysis_date}",
        file_type = "df",
        partitioned=True
    )
                  
    # filter to usable trips
    # pass valid_operator_vp down 
    # valid_operator_vp = delayed(helpers.exclude_unusable_trips)(
    # operator_vp_segments, trips_list)
    #logger.info(f"filter out to only valid trips: {}")
    
    vp_pared = delayed(segment_calcs.keep_min_max_timestamps_by_segment)(
        vp_joined_to_segments, 
        segment_identifier_cols = SEGMENT_IDENTIFIER_COLS,
        timestamp_col = TIMESTAMP_COL
    )
        
    # Unpack delayed results
    # store in a list to be unpacked
    results = [vp_pared]
    
    dask_utils.compute_and_export(
        results, 
        gcs_folder = SEGMENT_GCS,
        file_name = f"{EXPORT_FILE}_{analysis_date}",
        export_single_parquet=False
    )
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/A4_valid_vehicle_positions.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
   
    time1 = datetime.datetime.now()
    pare_down_vp_to_valid_trips(
        analysis_date,
        dict_inputs = ROUTE_SEG_DICT
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"pare down vp by route segments {time2 - time1}")
    
    pare_down_vp_to_valid_trips(
        analysis_date,
        dict_inputs = STOP_SEG_DICT
    )
    
    time3 = datetime.datetime.now()
    logger.info(f"pare down vp by stop segments {time3 - time2}")

    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")