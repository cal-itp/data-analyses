import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?

import datetime as dt
import gcsfs
import geopandas as gpd
import pandas as pd

from siuba import *
from tqdm import tqdm_notebook
from tqdm.notebook import trange, tqdm

import shared_utils
from rt_analysis import rt_parser as rt

fs = gcsfs.GCSFileSystem()

date = shared_utils.rt_dates.DATES["feb2022"]
analysis_date = pd.to_datetime(date).date()
pbar = tqdm()

from calitp.tables import tbls
from siuba import *


if __name__=="__main__":

    air_joined = pd.read_parquet(
        f"2022-10-12_working_organizations.parquet")
    
    day = str(analysis_date.day).zfill(2)
    month = str(analysis_date.month).zfill(2)
    
    fs_list = fs.ls(f'{shared_utils.rt_utils.GCS_FILE_PATH}rt_trips/')
    
    ## now finds ran operators on specific analysis date
    operators_status_dict = shared_utils.rt_utils.get_operators(
        date, list(air_joined.calitp_itp_id))
    
    ran_operators = [k for k, v in operators_status_dict.items() 
                     if v == 'already_ran']
    #{k: v for k, v in operators_status_dict.items() if v == 'not_yet_run'}
    
    not_ran_operators = []
    for agency in air_joined.calitp_itp_id.unique():
    # for agency in [208]:
        if agency in ran_operators:
            print(f'already ran: {agency}')
            continue
        ## https://docs.google.com/spreadsheets/d/16tcL3fPdYkrNajDNneSR1b5ImhFOCJ0gWFnEaXyg16A/edit#gid=0
        print(f'calculating for agency: {agency}...')
        try:
            rt_day = rt.OperatorDayAnalysis(agency, analysis_date, pbar)
            rt_day.export_views_gcs()
            print(f'complete for agency: {agency}')
        except Exception as e:
            print(f'rt failed for agency {agency}')
            not_ran_operators += [agency]
            print(e)