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
    #keep only 5311 programs 
    subset = ['Section 5311', '5311(f) Cont', 'CMAQ (FTA 5311)',
       'Section 5311(f)','5311(f) Round 2']
    df = df[df.funding_program.isin(subset)]
    
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
    #Add up columns 0-9 to get a new bin 
    vehicles['0-9'] = vehicles[[0,1,2,3,4,5,6,7,8,9]].sum(axis=1)
    #Add up columns 10-12
    vehicles['10-12'] = vehicles[[10,11,12]].sum(axis=1)
    #dropping columns: no need for 0-12 anymore
    vehicles = vehicles.drop(columns=[0,1,2,3,4,5,6,7,8,9,10,11,12])
    #only want rural reporters
    vehicles = vehicles.loc[vehicles['Reporter Type'] == 'Rural Reporter'] 
    #can convert to snakecase now
    vehicles = to_snakecase(vehicles)
    return vehicles

def load_organizations_data():
    organizations =  pd.read_csv('gs://calitp-analytics-data/data-analyses/5311 /organizations-all_organizations.csv')
    organizations = to_snakecase(organizations)
    #Only keeping relevant columns
    organizations = organizations[['name','ntp_id','itp_id','gtfs_schedule_status','#_services_w__complete_rt_status',
       '#_fixed_route_services_w__static_gtfs',
       'complete_static_gtfs_coverage__1=yes_', 'complete_rt_coverage',
       '>=1_gtfs_feed_for_any_service__1=yes_',
       '>=_1_complete_rt_set__1=yes_']]
    #Renaming columns 
    organizations = organizations.rename(columns = {'ntp_id':'ntd_id', 'name':'agency'})
    #dropping any agencies without NTD IDs
    organizations.dropna(subset=['ntd_id'])
    return organizations


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











