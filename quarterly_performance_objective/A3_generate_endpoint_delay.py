"""
Download RT endpoint delay
"""
import datetime
import gcsfs
import pandas as pd
import sys

# Add rt utils needed
sys.path.append("../rt_delay/")

from calitp.tables import tbl
from siuba import *
from loguru import logger

import rt_filter_map_plot
from shared_utils import rt_utils
from update_vars import ANALYSIS_DATE

logger.add("./logs/A3_generate_endpoint_delay.log")
logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")


DATA_PATH = "./data/"
fs = gcsfs.GCSFileSystem()

MONTH_DAY = f"{ANALYSIS_DATE.split('-')[1]}_{ANALYSIS_DATE.split('-')[2]}"

def operators_with_rt(selected_date: str):
    fs_list = fs.ls(f"{rt_utils.GCS_FILE_PATH}rt_trips/")

    ## now finds ran operators on specific analysis date

    ran_operators = [int(path.split('rt_trips/')[1].split('_')[0])
                     for path in fs_list if MONTH_DAY in path]
    
    return ran_operators


def aggregate_endpoint_delays(itp_id_list: list) -> pd.DataFrame:
    """
    Return endpoint delay df from rt_filter_map_plot.
    """
    endpoint_delay_all = pd.DataFrame()
    
    # Format it back into datetime
    analysis_date = pd.to_datetime(ANALYSIS_DATE).date()
    
    for itp_id in itp_id_list:
        rt_day = rt_filter_map_plot.from_gcs(itp_id, analysis_date)
        df = rt_day.endpoint_delay_view
        df['calitp_itp_id'] = itp_id
        
        endpoint_delay_all = pd.concat([endpoint_delay_all, df], axis=0)
    
    endpoint_delay_all = (endpoint_delay_all.sort_values("calitp_itp_id")
                          .reset_index(drop=True)
                         )
    
    return endpoint_delay_all



if __name__=="__main__":
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}")
    start = datetime.datetime.now()
    
    ran_operators = operators_with_rt(MONTH_DAY)
    
    aggregate_endpoint_delays = aggregate_endpoint_delays(ran_operators)
    
    time1 = datetime.datetime.now()
    logger.info(f"concatenate all endpoint delays for operators: {time1 - start}")

    aggregate_endpoint_delays.to_parquet(
        f"{DATA_PATH}endpoint_delays_{ANALYSIS_DATE}.parquet")
 
    logger.info(f"execution time: {datetime.datetime.now() - start}")

    '''
    airtable_organizations = (
        tbl.airtable.california_transit_organizations()
        >> select(_.itp_id, _.name, _.caltrans_district,
                  _.website, _.ntp_id, _.drmt_organization_name)
        >> arrange(_.caltrans_district, _.itp_id)
        >> collect()
        >> filter(_.itp_id.isin(ran_operators))
        >> mutate(itp_id = _.itp_id.astype('int64'), 
                  analysis_date = ANALYSIS_DATE
                 )
    )
    
    airtable_organizations.to_parquet(
        f"{DATA_PATH}{ANALYSIS_DATE.isoformat()}_working_organizations.parquet")

    '''
