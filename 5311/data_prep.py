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





