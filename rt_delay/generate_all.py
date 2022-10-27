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
import rt_analysis as rt

fs = gcsfs.GCSFileSystem()

date = shared_utils.rt_dates.DATES["aug2022"]
analysis_date = pd.to_datetime(date).date()
pbar = tqdm()

from calitp.tables import tbl
from siuba import *


if __name__=="__main__":

    '''
    airtable_organizations = (
        tbl.airtable.california_transit_organizations()
        >> select(_.itp_id)
        >> filter(_.itp_id != 200)
        >> collect()
    )


    (airtable_organizations[airtable_organizations.itp_id.notna()]
     .astype({"itp_id": int})
     .rename(columns = {"itp_id": "calitp_itp_id"})
     .to_parquet(f"{date}_working_organizations.parquet")
    )
    '''
    air_joined = pd.read_parquet(
        f"{date}_working_organizations.parquet")
    
    day = str(analysis_date.day).zfill(2)
    month = str(analysis_date.month).zfill(2)
    
    fs_list = fs.ls(f'{shared_utils.rt_utils.GCS_FILE_PATH}rt_trips/')
    
    ## now finds ran operators on specific analysis date
    operators_status_dict = shared_utils.rt_utils.get_operators(
        date, list(air_joined.calitp_itp_id))
    
    ran_operators = [k for k, v in operators_status_dict.items() if v == 'already_ran']
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