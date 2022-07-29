"""
Utils for Scheduled vs RT Trips
"""

import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000) ## 800GB?

from calitp.tables import tbl
from calitp import query_sql
import calitp.magics
import branca

import shared_utils

from siuba import *
import pandas as pd

import datetime as dt
import time
from zoneinfo import ZoneInfo

import importlib

import gcsfs
fs = gcsfs.GCSFileSystem()

from tqdm import tqdm_notebook
from tqdm.notebook import trange, tqdm


# Get the data for Scheduled Trips and RT Trips  

def load_schedule_data(start_date, end_date, itp_id):
    gtfs_daily = (
        tbl.views.gtfs_schedule_fact_daily_trips()
        >> filter(_.calitp_itp_id == itp_id)
        >> filter((_.service_date >= start_date),
                 (_.service_date <= end_date))
        >> filter(_.is_in_service == True)
        >> collect()
    )
    return gtfs_daily

def load_rt_data(start_date, end_date):
    rt = query_sql(
        f"""
        SELECT * FROM `cal-itp-data-infra-staging.natalie_views.test_rt_trips`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        """
    )
    return rt

# Get a DF with Percent Ran for each Day

def get_pct_ran_df(itp_id, list_of_dates, gtfs_daily, rt):
    
    pcts = []
    
    # loop through list of dates
    for single_date in list_of_dates:
        
        #filter for single day
        
        gtfs_daily2 = (gtfs_daily>>filter(_.service_date == single_date))
        rt2 = (rt>>filter(_.date == single_date))
        
        #outer join schedule and rt data 
        sched_rt_df = (pd.merge(gtfs_daily2, rt2, how='outer', on='trip_id', indicator='have_rt'))

        
        day_pct_ran = {}
        day_pct_ran['date'] = single_date
        if ((len(sched_rt_df))!=0):
            day_pct_ran['pct_trips_ran'] = ((len(sched_rt_df>>filter(_.have_rt=='both')))/(len(gtfs_daily2)))
        elif ((len(sched_rt_df))==0):
            day_pct_ran['pct_trips_ran'] = ''
        pct_ran = pd.DataFrame([day_pct_ran])
        
        # add columns with counts 
        pct_ran['n_have_rt'] = (len(sched_rt_df>>filter(_.have_rt=='both')))
        pct_ran['n_missing_rt'] = (len(sched_rt_df>>filter(_.have_rt=='right_only')))
        pct_ran['unmatched_rt'] = (len(sched_rt_df>>filter(_.have_rt=='left_only')))
        
        # add columns for number of unique trip_ids
        pct_ran['nunique_sched'] = (gtfs_daily2.trip_id.nunique())
        pct_ran['nunique_rt'] = (rt2.trip_id.nunique())

        pcts.append(pct_ran)                                                    
        #code help from: https://stackoverflow.com/questions/28669482/appending-pandas-dataframes-generated-in-a-for-loop
   
    #add each date together 
    pcts = pd.concat(pcts)
    
    #arrange by date
    pcts = pcts>>arrange(_.date)
    return pd.DataFrame(pcts)
