"""
Cleaning 5311 Agency Data

"""

import numpy as np
import pandas as pd
from siuba import *
from calitp import *
from plotnine import *
import intake

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/5311 /"


def load_grantprojects(): 
    File_5311 =  "Grant_Projects.xlsx"
    df = pd.read_excel(f'{GCS_FILE_PATH}{File_5311}')
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
    File_5311 =  "Grant_Projects.xlsx"
    df = pd.read_excel(f'{GCS_FILE_PATH}{File_5311}')
    pd.set_option('display.max_columns', None)
    
    #a bit of cleaning
    df = to_snakecase(df)
    df = df.drop(columns=['project_closed_by', 'project_closed_date', 'project_closed_time'])
    
    #get just the 5339 agencies
    df = (df>>filter(_.funding_program.str.contains('5339')))
    return df
        

def load_vehiclesdata():
    vehicles_info =  pd.read_excel('gs://calitp-analytics-data/data-analyses/5311 /2020-Vehicles_1.xlsm', sheet_name = ['Age Distribution'])
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
    #can convert to snakecase now
    vehicles = to_snakecase(vehicles)
    #making sure ntd_id is a string
    vehicles.ntd_id = vehicles.ntd_id.astype(str)
    #adding vehicle grouping function
    
    
    def get_vehicle_groups(vehicles):
        Automobiles = ['Automobile','Automobiles (Service)','Sports Utility Vehicle']
        Bus = ['Bus','Over-the-road Bus']
        Vans = ['Van','Trucks and other Rubber Tire Vehicles (Service)','Minivan','Cutaway']
        
        def replace_modes(row):
            if row.vehicle_type in Automobiles:
                return "Rail"
            elif row.vehicle_type in Bus:
                return "Bus"
            else:
                return "Vans"
        
        vehicles["vehicle_groups"] = vehicles.apply(lambda x: replace_modes(x), axis=1)
    
        return vehicles

    vehicles = (get_vehicle_groups(vehicles))
    
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
    organizations = organizations.rename(columns = {'ntp_id':'ntd_id','#_services_w__complete_rt_status':'number_services_w_complete_rt_status', 'complete_static_gtfs_coverage__1=yes_':'complete_static_gtfs_coverage_1_means_yes',     '>=1_gtfs_feed_for_any_service__1=yes_':'gt_1_gtfs_for_any_service_1_means_yes', '>=_1_complete_rt_set__1=yes_':'gt_1_complete_rt_set_1_means_yes'})
    #making sure ntd_id is a string
    organizations.ntd_id = organizations.ntd_id.astype(str)
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

def load_organizations_data2():
    File_Organizations =  "organizations-all_organizations.csv"
    organizations =  pd.read_csv(f'{GCS_FILE_PATH}{File_Organizations}')
    organizations = to_snakecase(organizations)
    #Only keeping relevant columns
    organizations = organizations[['name','ntp_id','itp_id','gtfs_schedule_status','#_services_w__complete_rt_status',
       '#_fixed_route_services_w__static_gtfs',
       'complete_static_gtfs_coverage__1=yes_', 'complete_rt_coverage',
       '>=1_gtfs_feed_for_any_service__1=yes_',
       '>=_1_complete_rt_set__1=yes_']]
   #Renaming columns 
    organizations = organizations.rename(columns = {'ntp_id':'ntd_id','#_services_w__complete_rt_status':'number_services_w_complete_rt_status', 'complete_static_gtfs_coverage__1=yes_':'complete_static_gtfs_coverage_1_means_yes',     '>=1_gtfs_feed_for_any_service__1=yes_':'gt_1_gtfs_for_any_service_1_means_yes', '>=_1_complete_rt_set__1=yes_':'gt_1_complete_rt_set_1_means_yes'})
    #making sure ntd_id is a string
    organizations.ntd_id = organizations.ntd_id.astype(str)
    return organizations

def load_vehiclesdata2():
    File_Vehicles =  "2020-Vehicles_1.xlsm"
    vehicles_info =  pd.read_excel(f'{GCS_FILE_PATH}{File_Vehicles}',
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
    #can convert to snakecase now
    vehicles = to_snakecase(vehicles)
    #making sure ntd_id is a string
    vehicles.ntd_id = vehicles.ntd_id.astype(str)
    #adding vehicle grouping function
    
    
    def get_vehicle_groups(vehicles):
        Automobiles = ['Automobile','Automobiles (Service)','Sports Utility Vehicle']
        Bus = ['Bus','Over-the-road Bus']
        Vans = ['Van','Trucks and other Rubber Tire Vehicles (Service)','Minivan','Cutaway']
        
        def replace_modes(row):
            if row.vehicle_type in Automobiles:
                return "Rail"
            elif row.vehicle_type in Bus:
                return "Bus"
            else:
                return "Vans"
        
        vehicles["vehicle_groups"] = vehicles.apply(lambda x: replace_modes(x), axis=1)
    
        return vehicles

    vehicles = (get_vehicle_groups(vehicles))
    
    return vehicles

def load_cleaned_organizations_data():
    File_Organization_Clean =  "organizations_cleaned.csv"
    organizations =  pd.read_csv(f'{GCS_FILE_PATH}{File_Organization_Clean}')
    organizations = to_snakecase(organizations)
    #Only keeping relevant columns
    organizations = organizations[['name','ntp_id','itp_id','gtfs_schedule_status','#_services_w__complete_rt_status',
       '#_fixed_route_services_w__static_gtfs',
       'complete_static_gtfs_coverage__1=yes_', 'complete_rt_coverage',
       '>=1_gtfs_feed_for_any_service__1=yes_',
       '>=_1_complete_rt_set__1=yes_']]
   #Renaming columns 
    organizations = organizations.rename(columns = {'ntp_id':'ntd_id','#_services_w__complete_rt_status':'number_services_w_complete_rt_status', 'complete_static_gtfs_coverage__1=yes_':'complete_static_gtfs_coverage_1_means_yes',     '>=1_gtfs_feed_for_any_service__1=yes_':'gt_1_gtfs_for_any_service_1_means_yes', '>=_1_complete_rt_set__1=yes_':'gt_1_complete_rt_set_1_means_yes'})
    #making sure ntd_id is a string
    organizations.ntd_id = organizations.ntd_id.astype(str)
    return organizations



