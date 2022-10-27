"""
Download RT endpoint delay
"""
import datetime
import glob
import os
import pandas as pd
import sys

from loguru import logger

from shared_utils import rt_utils, utils, gtfs_utils
from rt_analysis import rt_filter_map_plot

from update_vars import ANALYSIS_DATE, COMPILED_CACHED_GCS

logger.add("./logs/A3_generate_endpoint_delay.log")
logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")


def aggregate_endpoint_delays(itp_id_list: list) -> pd.DataFrame:
    """
    Return endpoint delay df from rt_filter_map_plot.
    """
    endpoint_delay_all = pd.DataFrame()
    
    # Format it back into datetime
    analysis_date = pd.to_datetime(ANALYSIS_DATE).date()
    
    for itp_id in itp_id_list:
        try:
            logger.info(f"start {itp_id}")
            rt_day = rt_filter_map_plot.from_gcs(itp_id, analysis_date)
            df = rt_day.endpoint_delay_view

            df['calitp_itp_id'] = itp_id
            df.to_parquet(f"./data/endpoint_{itp_id}.parquet")
            endpoint_delay_all = pd.concat([endpoint_delay_all, df], axis=0)
        except:
            logger.warning(f"error: {itp_id}")
            continue
    
    endpoint_delay_all = (endpoint_delay_all.sort_values("calitp_itp_id")
                          .reset_index(drop=True)
                         )
    
    return endpoint_delay_all


def remove_local_files():
    files = glob.glob("../data/endpoint_*.parquet")
    
    for f in files:
        os.remove(f)

        
if __name__=="__main__":
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}")
    start = datetime.datetime.now()
    
    operators_status_dict = rt_utils.get_operators(
        ANALYSIS_DATE, gtfs_utils.ALL_ITP_IDS)
    
    #erroring_operators = [14, 206]
    ran_operators = [k for k, v in operators_status_dict.items() 
                     if v == 'already_ran']

    aggregate_endpoint_delays = aggregate_endpoint_delays(ran_operators)
    
    time1 = datetime.datetime.now()
    logger.info(f"concatenate all endpoint delays for operators: {time1 - start}")
    
    # Save in GCS
    utils.geoparquet_gcs_export(
        aggregate_endpoint_delays,
        COMPILED_CACHED_GCS,
        f"endpoint_delays_{ANALYSIS_DATE}"
    )
    
    remove_local_files()
 
    logger.info(f"execution time: {datetime.datetime.now() - start}")
