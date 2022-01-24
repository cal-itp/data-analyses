"""
Cleaning 5311 Agency Data

"""

import numpy as np
import pandas as pd
from siuba import *
from calitp import *
from plotnine import *
import intake


def load_grantprojects(): 
    df = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/5311 /Grant_Projects.xlsx', sheet_name=None), ignore_index=True)
    
    pd.set_option('display.max_columns', None)
    
    #a bit of cleaning
    df = to_snakecase(df)
    df = df.drop(columns=['project_closed_by', 'project_closed_date', 'project_closed_time'])
    
    #get just the rural agencies
    df = (df>>filter(_.reporter_type=='Rural Reporter'))
    
    return df

def load_grantprojects5339(): 
    df = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/5311 /Grant_Projects.xlsx', sheet_name=None), ignore_index=True)
    
    pd.set_option('display.max_columns', None)
    
    #a bit of cleaning
    df = to_snakecase(df)
    df = df.drop(columns=['project_closed_by', 'project_closed_date', 'project_closed_time'])
    
    #get just the 5339 agencies
    df = (df>>filter(_.funding_program.str.contains('5339')))
    
    return df


def load_vehiclesdata():
    vehicles_info =  pd.read_excel('gs://calitp-analytics-data/data-analyses/5311 /2020-Vehicles_1.xlsm',
                                   sheet_name = ['Age Distribution'])
    pd.set_option('display.max_columns', None)
    #cannot use to_snakecase because of integer column names
    vehicles_info = vehicles_info['Age Distribution']
    vehicles = (vehicles_info>>filter(_.State=='CA'))
    
    return vehicles



def load_agencyinfo(): 
    agencies = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/5311 /2020_Agency_Information.xlsx',
                                      sheet_name=None),ignore_index=True)
    agencies = to_snakecase(agencies)
    agency_info = agencies>>filter(_.state=='CA')
    
    return agency_info


def load_catalog_gtfs(): 
    
    catalog = intake.open_catalog("catalog.yml")
    gtfs_status = catalog.gtfs_status.read()
    gtfs_status = to_snakecase(gtfs_status)
    
    
    return gtfs_status 











