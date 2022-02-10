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

def load_agencyinfo(): 
    agencies = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/5311 /2020_Agency_Information.xlsx',
                                      sheet_name=None),ignore_index=True)
    agencies = to_snakecase(agencies)
    agency_info = agencies>>filter(_.state=='CA')
    
    return agency_info

def GTFS():
    GTFS_File =  "services-GTFS Schedule Status.csv"
    GTFS =  pd.read_csv(f'{GCS_FILE_PATH}{GTFS_File}')
    GTFS = to_snakecase(GTFS)
    #Only keeping relevant columns
    GTFS = GTFS [['name','provider','operator','gtfs_schedule_status']]
    #rename to avoid confusion
    GTFS = GTFS.rename(columns = {'gtfs_schedule_status':'simple_GTFS_status'})     
    #drop GTFS with nas
    GTFS = GTFS.dropna(subset=['simple_GTFS_status'])
    #some agencies have "" replace with space
    GTFS['provider'] = GTFS['provider'].str.replace('"', "")
    return GTFS

"""
Merged Data

"""
#Merged dataframe objects
#crosswalk dictionary for merging data frame
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

#Agencies that ONLY appear in Black Cat
BC_Only = ['County of Los Angeles - Department of Public Works',
 'County of Nevada Public Works, Transit Services Division',
 'County of Sacramento Department of Transportation',
 'Glenn County Transportation Commission',
 'Stanislaus County Public Works - Transit Division',
 'Alpine County Community Development',
 'Fresno Council of Governments',
 'Greyhound Lines, Inc.']

#Dataframe merge, commenting out for now.
'''
def merged_dataframe():
    #call data frames we have to join.
    df_5311 = data_prep.load_grantprojects()
    vehicles = data_prep.load_cleaned_vehiclesdata()
    organizations = data_prep.load_cleaned_organizations_data()
    #merge vehicles from NTD & GTFS
    vehicles_gtfs = pd.merge(vehicles, organizations,  how='left', on=['ntd_id'])
    #left merge, Black Cat on the left and vehicle_gtfs on the right. 
    Test1 = (pd.merge(df_5311, vehicles_gtfs,  how='left', left_on=['organization_name'], 
                      right_on=['name'], indicator=True)
            )
    #Filter out for left only matches, make it into a list
    Left_only = Test1[(Test1._merge.str.contains("left_only", case= False))] 
    Left_orgs = Left_only['organization_name'].drop_duplicates().tolist()
    #Delete  left only matches from original df 
    m2 = Test1[~Test1.organization_name.isin(Left_orgs)]
    #making a data frame with only failed merges out out of original Black Cat
    fail = df_5311[df_5311.organization_name.isin(Left_orgs)]
    #replacing organization names from Black Cat with agency names from NTD Vehicles.
    fail['organization_name'].replace(crosswalk, inplace= True)
    #Merging the failed organizations to vehicles 
    Test2 = pd.merge(fail, vehicles_gtfs,  how='left', left_on=['organization_name'], right_on=['agency'])
    #appending failed matches to the first data frame
    BC_GTFS_NTD = m2.append(Test2, ignore_index=True)
    #create a column to flag agencies that appear in black cat only
    def BC_only(row):
    if row.organization_name in BC_Only:
        return '1'
    else: 
        return '0'  
    BC_GTFS_NTD["Is_Agency_In_BC_Only_1_means_Yes"] = BC_GTFS_NTD.apply(lambda x: BC_only(x), axis=1)
    #replace GTFS and cal itp for Klamath. Klamath does not appear in NTD data.
    BC_GTFS_NTD.loc[(BC_GTFS_NTD['organization_name'] == 'Klamath Trinity Non-Emergency Transportation\u200b'), "itp_id"] = "436"
    BC_GTFS_NTD.loc[(BC_GTFS_NTD['organization_name'] == 'Klamath Trinity Non-Emergency Transportation\u200b'), "gtfs_schedule_status"] = "needed"
    #get a more definitive GTFS status: ok, needed, long term solution needed, research
    temp = BC_GTFS_NTD.gtfs_schedule_status.fillna("None")
    BC_GTFS_NTD['GTFS_schedule_status_use'] = np.where(temp.str.contains("None"),"None",
                   np.where(temp.str.contains("ok"), "Ok",
                   np.where(temp.str.contains("long"), "Long-term solution needed",
                   np.where(temp.str.contains("research"), "Research", "Needed"))))
    #### FLEET SIZE STUFF #####
    #delete old columns
    BC_GTFS_NTD = BC_GTFS_NTD.drop(columns=['gtfs_schedule_status','name','agency'])
    #rename
    BC_GTFS_NTD = BC_GTFS_NTD.rename(columns = {'GTFS_schedule_status_use':'GTFS'})
    #get agencies without any data to show up when grouping
    show_up = ['reporter_type']
    for i in show_up:
        BC_GTFS_NT2[i] = BC_GTFS_NTD[i].fillna('None')
    #change itp id to be float.
    BC_GTFS_NTD['itp_id'] = BC_GTFS_NTD['itp_id'].fillna(0)
    BC_GTFS_NTD.loc[(BC_GTFS_NTD['itp_id'] == '436'), "itp_id"] = 436
    #save into parquet
    BC_GTFS_NTD.to_parquet
return BC_GTFS_NTD 
'''

'''
charting functions 
'''
#Labels
def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    LABEL_DICT = { "prepared_y": "Year",
              "dist": "District",
              "nunique":"Number of Unique",
              "project_no": "Project Number"}
    
    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    elif word in LABEL_DICT.keys():
        word = LABEL_DICT[word]
    else:
        #word = word.replace('n_', 'Number of ').title()
        word = word.replace('unique_', "Number of Unique ").title()
        word = word.replace('_', ' ').title()
    
    return word

# Bar
def basic_bar_chart(df, x_col, y_col):
    
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), sort=('-y')),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 #column = "payment:N",
                 color = alt.Color(y_col,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(y_col)))
                                  ))
             .properties( 
                          title=(f"Highest {labeling(x_col)} by {labeling(y_col)}"))
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./chart_outputs/bar_{x_col}_by_{y_col}.png")
    
    return chart


# Scatter 
def basic_scatter_chart(df, x_col, y_col, colorcol):
    
    chart = (alt.Chart(df)
             .mark_circle(size=60)
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 #column = "payment:N",
                 color = alt.Color(colorcol,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ))
             .properties( 
                          title = (f"Highest {labeling(x_col)} by {labeling(y_col)}"))
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./chart_outputs/scatter_{x_col}_by_{y_col}.png")
    
    return chart


# Line
def basic_line_chart(df, x_col, y_col):
    
    chart = (alt.Chart(df)
             .mark_line()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col))
                                   )
              ).properties( 
                          title=f"{labeling(x_col)} by {labeling(y_col)}")

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./chart_outputs/line_{x_col}_by_{y_col}.png")
    
    return chart


# Bar chart without highest f'string
def basic_bar_chart2(df, x_col, y_col):
    
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), sort=('-y')),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 #column = "payment:N",
                 color = alt.Color(y_col,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(y_col)))
                                  ))
             .properties( 
                          title=(f"{labeling(x_col)} by {labeling(y_col)}"))
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./chart_outputs/bar_{x_col}_by_{y_col}.png")
    
    return chart

'''

Aggregating Functions

'''
#Aggregate by fleet size, GTFS, vehicle ages.
def aggregation_one(df, grouping_col):
    #adding up the vehicles 9+ and 15+ 
    df['vehicles_older_than_9']= df['_10_12'] + df['_13_15'] + df['_16_20'] + df['_21_25'] + df['_26_30'] + df['_31_60'] + df['_60plus']
    df['vehicles_older_than_15']= df['_16_20'] + df['_21_25'] + df['_26_30'] + df['_31_60'] + df['_60plus']
    #rename 0-9
    df = df.rename(columns={'_0_9':'vehicles_0_to_9'}) 
    #pivot 
    df = df.groupby([grouping_col]).agg({'vehicles_older_than_9':'sum', 'vehicles_older_than_15':'sum', 'vehicles_0_to_9': 'sum'}) 
    #dividing the different bins by the total across all agencies
    df['vehicles_percent_older_than_9'] = (df['vehicles_older_than_9']/sum(df['vehicles_older_than_9']))*100
    df['vehicles_percent_older_than_15'] = (df['vehicles_older_than_15']/sum(df['vehicles_older_than_15']))*100
    df['vehicles_percent_0_to_9'] = (df['vehicles_0_to_9']/sum(df['vehicles_0_to_9']))*100
    #reset index
    df = df.reset_index()
    return df 