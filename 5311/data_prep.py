"""
Cleaning 5311 Agency Data

"""

import numpy as np
import pandas as pd
from siuba import *
from calitp import *
from plotnine import *
import intake
from shared_utils import geography_utils

import altair as alt
import altair_saver
from shared_utils import geography_utils
from shared_utils import altair_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide

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

def load_catalog_gtfs(): 
    
    catalog = intake.open_catalog("catalog.yml")
    gtfs_status = catalog.gtfs_status.read()
    gtfs_status = to_snakecase(gtfs_status)
    
    
    return gtfs_status 

def load_cleaned_vehiclesdata():
    File_Vehicles =  "cleaned_vehicles.xlsx"
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
        Automobiles = ['Automobile','Sports Utility Vehicle']
        Bus = ['Bus','Over-the-road Bus','Articulated Bus','Double Decker Bus','Trolleybus']
        Vans = ['Van','','Minivan','Cutaway']
        Trains = ['Vintage Trolley','Automated Guideway Vehicle','Heavy Rail Passenger Car','Light Rail Vehicle',
                 'Commuter Rail Self-Propelled Passenger Car','Commuter Rail Passenger Coach','Commuter Rail Locomotive',
                'Cable Car']
        Service = ['Automobiles (Service)',
                   'Trucks and other Rubber Tire Vehicles (Service)',
                   'Steel Wheel Vehicles (Service)']
        other = ['Other','Ferryboat']
        
        def replace_modes(row):
            if row.vehicle_type in Automobiles:
                return "Automobiles"
            elif row.vehicle_type in Bus:
                return "Bus"
            elif row.vehicle_type in Trains:
                return "Train"
            elif row.vehicle_type in Vans:
                return "Van"
            elif row.vehicle_type in Service:
                return "Service"
            else:
                return "Other"
        
        vehicles["vehicle_groups"] = vehicles.apply(lambda x: replace_modes(x), axis=1)
    
        return vehicles
    
    def get_age_and_doors(df):   

        df = df.rename(columns={'_60+': '_60plus'})

        age = geography_utils.aggregate_by_geography(df, 
                           group_cols = ["agency", "ntd_id", "reporter_type"],
                           sum_cols = ["total_vehicles", "_0_9","_10_12", "_13_15", "_16_20","_21_25","_26_30","_31_60","_60plus"],
                             mean_cols = ["average_age_of_fleet__in_years_", "average_lifetime_miles_per_vehicle"]
                                          ).sort_values(["agency","total_vehicles"], ascending=[True, True])
    

        older = (age.query('_21_25 != 0 or _26_30 != 0 or _31_60 != 0 or _60plus!=0'))
        older["sum_15plus"] = older[["_16_20","_21_25","_26_30","_31_60","_60plus"]].sum(axis=1)
        older = (older>>select(_.agency, _.sum_15plus))

        age = pd.merge(age, older, on=['agency'], how='left')

        types = (df
                 >>select(_.agency, _.vehicle_groups, _._0_9, _._10_12, _._13_15, _._16_20, _._21_25, _._26_30, _._31_60, _._60plus))
        types['sum_type'] = types[['_0_9', '_10_12', '_13_15', '_16_20', '_21_25','_26_30','_31_60','_60plus']].sum(axis=1)
        #https://towardsdatascience.com/pandas-pivot-the-ultimate-guide-5c693e0771f3
        types = (types.pivot_table(index="agency", columns="vehicle_groups", values="sum_type", aggfunc=np.sum, fill_value=0)).reset_index()

        types['automobiles_door']= (types['Automobiles']*2)
        types['bus_doors']= (types['Bus']*2)
        types['train_doors']=(types['Train']*2)
        types['van_doors']=(types['Van']*1)

        types["doors_sum"] = types[["automobiles_door","bus_doors","train_doors","van_doors"]].sum(axis=1)

        agency_counts = pd.merge(age, types, on=['agency'], how='left')
    
          

        return agency_counts

    vehicles = (get_vehicle_groups(vehicles))
    vehicles = (get_age_and_doors(vehicles))

    
    return vehicles

def load_cleaned_organizations_data():
    File_Organization_Clean =  "organizations_cleaned.csv"
    organizations =  pd.read_csv(f'{GCS_FILE_PATH}{File_Organization_Clean}')
    organizations = to_snakecase(organizations)
    #Only keeping relevant columns
    organizations = organizations[['name','ntp_id','itp_id','gtfs_schedule_status']]
    #Renaming columns 
    organizations = organizations.rename(columns = {'ntp_id':'ntd_id'})
    #making sure ntd_id is a string
    organizations.ntd_id = organizations.ntd_id.astype(str)
    return organizations

def load_airtable():
    File_Airtable = "organizations-AllOrganizations_1.csv"
    Airtable =  pd.read_csv(f'{GCS_FILE_PATH}{File_Airtable}')
    Airtable = to_snakecase(Airtable)
    Airtable = Airtable[['name', 'caltrans_district', 'mpo_rtpa', 'planning_authority',
       'gtfs_static_status', 'gtfs_realtime_status']]
    return Airtable

"""
Merged Data

"""
#crosswalk dictionary for function merged_dataframe()
crosswalk = {'City of Chowchilla ': 'City of Chowchilla, dba: Chowchilla Area Transit ',
     'City of Dinuba ':  'City of Dinuba',
     'Modoc Transportation Agency': 'Modoc Transportation Agency',
     'Butte County Association of Governments/ Butte Regional Transit': 'Butte County Association of Governments',
     'Calaveras County Public Works':  'Calaveras Transit Agency',
     'City of Escalon ':  'City of Escalon, dba: eTrans',
     'County of Mariposa':  'Mariposa County Transit, dba: Mari-Go',
     'County of Shasta Department of Public Works':  'County of Shasta Department of Public Works',
     'County of Siskiyou': 'County of Siskiyou, dba: Siskiyou County Transit',
     'County of Tulare': 'Tulare County Area Transit',
     'Eureka Transit Service':  'City of Eureka, dba: Eureka Transit Service',
     'Kern Regional Transit':  'Kern Regional Transit',
     'Livermore Amador Valley Transit Authority':  'Livermore / Amador Valley Transit Authority',
     'Placer County Public Works (TART & PCT)': 'County of Placer, dba: Placer County Department of Public Works',
     'Plumas County Transportation Commission': 'Plumas County Transportation Commission',
     'San Luis Obispo Regional Transit Authority':  'San Luis Obispo Regional Transit Authority',
     'Sonoma County Transit':  'County of Sonoma, dba: Sonoma County Transit',
     'Sunline Transit Agency':  'SunLine Transit Agency',
     'Tehama County Transit Agency': 'Tehama County',
     'Trinity County Department of Transportation ':  'Trinity County',
     'Tuolumne County Transit Agency (TCTA)':  'Tuolumne County Transit',
     'Amador Transit':  'Amador Regional Transit System',
     'City of Corcoran - Corcoran Area Transit':  'City of Corcoran, dba: Corcoran Area Transit',
     'Yosemite Area Regional Transportation System ':  'Yosemite Area Regional Transportation System',
     'County Connection (Central Contra Costa Transit Authority)': 'Central Contra Costa Transit Authority, dba: COUNTY CONNECTION',
     'Calaveras Transit Agency ': 'Calaveras Transit Agency'}

#Agencies that ONLY appear in Black Cat  for function merged_dataframe()
BC_Agency_List = ['County of Los Angeles - Department of Public Works',
 'County of Nevada Public Works, Transit Services Division',
 'County of Sacramento Department of Transportation',
 'Glenn County Transportation Commission',
 'Stanislaus County Public Works - Transit Division',
 'Alpine County Community Development',
 'Fresno Council of Governments',
 'Greyhound Lines, Inc.']

### FULL DATA SET ###
#Merging all 3 data frames, without any aggregation. There are ~800 rows of data from 2011-2021.
def merged_dataframe():
    ### MERGING ###
    #call the 3 data frames we have to join.
    df_5311 = load_grantprojects()
    vehicles = load_cleaned_vehiclesdata()
    organizations = load_cleaned_organizations_data()
    #merge vehicles from NTD & GTFS
    vehicles_gtfs = pd.merge(vehicles, organizations,  how='left', on=['ntd_id'])
    #left merge, Black Cat on the left and vehicle_gtfs on the right. 
    m1 = (pd.merge(df_5311, vehicles_gtfs,  how='left', left_on=['organization_name'], 
                      right_on=['name'], indicator=True)
            )
    
    #Filter out for left only matches, make it into a list
    Left_only = m1[(m1._merge.str.contains("left_only", case= False))] 
    Left_orgs = Left_only['organization_name'].drop_duplicates().tolist()
    #Delete  left only matches from original df 
    m2 = m1[~m1.organization_name.isin(Left_orgs)]
    #making a data frame with only failed merges out out of original Black Cat
    fail = df_5311[df_5311.organization_name.isin(Left_orgs)]
    #replacing organization names from Black Cat with agency names from NTD Vehicles.
    fail['organization_name'].replace(crosswalk, inplace= True)
    #Merging the failed organizations to vehicles 
    Test = pd.merge(fail, vehicles_gtfs,  how='left', left_on=['organization_name'], right_on=['agency'])
    #appending failed matches to the first data frame
    BC_GTFS_NTD = m2.append(Test, ignore_index=True)
    
    ### FLAG TO SHOW BLACK CAT ONLY AGENCIES ###
    #Function
    def BC_only(row):
        if row.organization_name in BC_Agency_List:
            return '1'
        else: 
            return '0'  
    BC_GTFS_NTD["Is_Agency_In_BC_Only_1_means_Yes"] = BC_GTFS_NTD.apply(lambda x: BC_only(x), axis=1)
    
    ###Replace GTFS and cal itp for Klamath###
    #Klamath does not appear in NTD data so we missed it when we merged NTD & Cal ITP on NTD ID
    BC_GTFS_NTD.loc[(BC_GTFS_NTD['organization_name'] == 'Klamath Trinity Non-Emergency Transportation\u200b'), "itp_id"] = "436"
    BC_GTFS_NTD.loc[(BC_GTFS_NTD['organization_name'] == 'Klamath Trinity Non-Emergency Transportation\u200b'), "gtfs_schedule_status"] = "needed"
    
    ###GTFS###
    #get a more definitive GTFS status: ok, needed, long term solution needed, research
    temp = BC_GTFS_NTD.gtfs_schedule_status.fillna("None")
    BC_GTFS_NTD['GTFS_schedule_status_use'] = np.where(temp.str.contains("None"),"None",
                   np.where(temp.str.contains("ok"), "Ok",
                   np.where(temp.str.contains("long"), "Long-term solution needed",
                   np.where(temp.str.contains("research"), "Research", "Needed"))))
    
    ###FLEET SIZE METRIC ### 
    #First grabbing only one row for each agency into a new data frame 
    Fleet_size = BC_GTFS_NTD.groupby(['organization_name',]).agg({'total_vehicles':'max'}).reindex()
    #Get percentiles in objects for total vehicle.
    p75 = Fleet_size.total_vehicles.quantile(0.75).astype(float)
    p25 = Fleet_size.total_vehicles.quantile(0.25).astype(float)
    p50 = Fleet_size.total_vehicles.quantile(0.50).astype(float)
    #Function for fleet size
    def fleet_size (row):
        if ((row.total_vehicles > 0) and (row.total_vehicles < p25)):
            return "Small"
        elif ((row.total_vehicles > p25) and (row.total_vehicles < p75)):
            return "Medium"
        elif ((row.total_vehicles > p50) and (row.total_vehicles > p75 )):
               return "Large"
        else:
            return "No Info"
    BC_GTFS_NTD["fleet_size"] = BC_GTFS_NTD.apply(lambda x: fleet_size(x), axis=1)
    
    ### FINAL CLEAN UP ###
    #delete old columns
    BC_GTFS_NTD = BC_GTFS_NTD.drop(columns=['gtfs_schedule_status','name','agency'])
    #rename
    BC_GTFS_NTD = BC_GTFS_NTD.rename(columns = {'GTFS_schedule_status_use':'GTFS'})
    #get agencies without any data to show up when grouping
    show_up = ['reporter_type']
    for i in show_up:
        BC_GTFS_NTD[i] = BC_GTFS_NTD[i].fillna('None')
    #change itp id to be floats & 0 so parquet will work
    BC_GTFS_NTD['itp_id'] = BC_GTFS_NTD['itp_id'].fillna(0)
    BC_GTFS_NTD.loc[(BC_GTFS_NTD['itp_id'] == '436'), "itp_id"] = 436
    #save into parquet
    #BC_GTFS_NTD.to_parquet("BC_GTFS_NTD.parquet")
    return BC_GTFS_NTD 

### AGGGREGATED MERGED DATA SET ###
#this function takes the full data frame above & sums it down to just one row for each organization with aggregated statistics. GTFS, district, planning authority, and mppo rtpa info also included
#this function takes the merged data frame above, which has around 700 rows & 
#sums it down to just one row for each organization with aggregated statistics
def aggregated_merged_df(): 
    #read original df & airtable
    df_original = merged_dataframe()
    airtable = load_airtable()
    
    # Grab the reporter type, fleet size, ntd id, and itp id to subset out
    df_subset = df_original[['organization_name','reporter_type', 'fleet_size', 'ntd_id', 'itp_id']]
    
    #aggregate vehicles 16+ above. 
    column_names = ['_16_20', '_21_25', '_26_30', '_31_60']
    df_original['_16plus']= df_original[column_names].sum(axis=1)
    
    #grab the max of all vehicle information & sum of all monetary cols from Black Cat
    df_original = df_original.groupby(['organization_name',]).agg({'allocationamount':'sum',
                                         'encumbered_amount':'sum',
                                         'expendedamount':'sum',
                                         'encumbered_amount':'sum',
                                         'activebalance':'sum',
                                         'closedoutbalance':'sum',
                                         'expendedamount':'sum',
                                         'total_vehicles':'max',
                                         'average_age_of_fleet__in_years_':'max',
                                         'average_lifetime_miles_per_vehicle':'max',
                                         'Automobiles':'max',
                                         'Bus':'max',
                                         'Other':'max',
                                         'Train':'max',
                                         'Van':'max',
                                         'automobiles_door':'max',
                                         'bus_doors':'max',
                                         'van_doors':'max',
                                         'train_doors':'max',
                                         'doors_sum':'max',
                                         '_0_9':'max',
                                         '_10_12':'max',
                                         '_13_15':'max',
                                         '_16_20':'max',
                                         '_21_25':'max',
                                         '_26_30':'max',
                                         '_31_60':'max',
                                         '_16plus':'max',
                                         '_60plus':'max'}).reset_index()
   
    df_subset = df_subset.drop_duplicates()
    
    #Merge the 2 dfs together
    df_merge1 = pd.merge(df_original,df_subset, how='left', left_on='organization_name', right_on = 'organization_name')
    
    #Merge df_merge1 with Air table that contains MPO, planning authority, more definitive GTFS status
    df_merge2 = pd.merge(df_merge1, airtable, how='left', left_on='organization_name', right_on='name')
    
    #GTFS is now split between gtfs_static & gtfs_realtime, create groups to show orgs with both
    #orgs with only either RT/Static, orgs with none
    def gtfs_status_indicator(df):   
        if (df['gtfs_static_status'] == 'Static OK') and (df['gtfs_realtime_status'] == "RT OK"):
            return 'Both static & RT OK'
        elif (df['gtfs_static_status'] == 'Static Incomplete') and (df['gtfs_realtime_status'] == "RT OK"):
            return 'Only RT'
        elif (df['gtfs_static_status'] == 'Static OK') and (df['gtfs_realtime_status'] == "RT Incomplete"):
            return 'Only Static'
        else: 
            return "None"
    df_merge2['GTFS_Status'] = df_merge2.apply(gtfs_status_indicator, axis = 1)
    
    #Drop unnecessary cols
    df_merge2 = df_merge2.drop('name', 1)
    return df_merge2 