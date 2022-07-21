import numpy as np
import pandas as pd
from siuba import *
from calitp import *
from plotnine import *
import intake
import cpi
import altair as alt
import altair_saver
from shared_utils import geography_utils
from shared_utils import altair_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide
import _agency_crosswalk as agency_crosswalk

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/5311 /"

"""
Importing the Data
"""
#5311 data from Black Cat
def load_grantprojects(): 
    File_5311 =  "Grant_Projects.xlsx"
    df = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}{File_5311}'))
    df = df.drop(columns=['project_closed_by', 'project_closed_date', 'project_closed_time'])
    #keep only 5311 programs 
    subset = ['Section 5311', '5311(f) Cont', 'CMAQ (FTA 5311)',
       'Section 5311(f)','5311(f) Round 2']
    df = df[df.funding_program.isin(subset)]
    return df

#Load Catalog
def load_catalog_gtfs(): 
    catalog = intake.open_catalog("catalog.yml")
    gtfs_status = catalog.gtfs_status.read()
    gtfs_status = to_snakecase(gtfs_status) 
    return gtfs_status 

#Airtable data with organization name and NTD ID. 
#Cleaned up orgs name manually in Excel back in January because had issues merging. 
def load_cleaned_organizations_data():
    File_Organization_Clean =  "organizations_cleaned.csv"
    organizations =  to_snakecase(pd.read_csv(f'{GCS_FILE_PATH}{File_Organization_Clean}'))
    #Only keep relevant columns
    organizations = organizations[['name','ntp_id','itp_id','gtfs_schedule_status']]
    #Rename NTD ID to its proper name  
    organizations = organizations.rename(columns = {'ntp_id':'ntd_id'})
    #Make sure ntd_id is a string
    organizations.ntd_id = organizations.ntd_id.astype(str)
    return organizations

#Second airtable from Cal ITP: no NTD ids, but other info like Caltrans distrct, 
#https://airtable.com/appPnJWrQ7ui4UmIl/tblFsd8D5oFRqep8Z/viwVBVSd0ZhYu8Ewm?blocks=hide
#Go to "All Views" -> "More collaborative views" -> "All Organizations" to download this.
def load_airtable():
    File_Airtable = "organizations-AllOrganizations_1.csv"
    Airtable = to_snakecase(pd.read_csv(f'{GCS_FILE_PATH}{File_Airtable}'))
    #Only keeping relevant columns
    Airtable = Airtable[['name', 'caltrans_district', 'mpo_rtpa', 'planning_authority',
       'gtfs_static_status', 'gtfs_realtime_status']]
    return Airtable

#Vehicles data from NTD
def load_vehicle_data():
    File_Vehicles =  "cleaned_vehicles.xlsx"
    vehicles_info =  pd.read_excel(f'{GCS_FILE_PATH}{File_Vehicles}',
                                   sheet_name = 'Age Distribution')
    vehicles = (vehicles_info>>filter(_.State=='CA'))
    
    return vehicles

"""
Metric/ETC Functions
Inflation Functions uses 2021 currency as base
"""
#Change this values to reflect your year of interest
base_year = 2021
current_year_dollars = 270.97

# Inflation table
def inflation_table(base_year):
    cpi.update()
    series_df = cpi.series.get(area="U.S. city average").to_dataframe()
    inflation_df = (series_df[series_df.year >= 2008]
           .pivot_table(index='year', values='value', aggfunc='mean')
           .reset_index()
          )
    denominator = inflation_df.value.loc[inflation_df.year==base_year].iloc[0]

    inflation_df = inflation_df.assign(
        inflation = inflation_df.value.divide(denominator)
    )
    
    return inflation_df

def adjust_prices(df):
    #Monetary columns
    cols =  ["allocationamount",
             "encumbered_amount",
             "expendedamount",
             "activebalance",
             "closedoutbalance"]
    
    ##get cpi table 
    cpi = inflation_table(base_year)
    cpi.update
    cpi = (cpi>>select(_.year, _.value))
    cpi_dict = dict(zip(cpi['year'], cpi['value']))
    
    
    for col in cols:
        multiplier = df["project_year"].map(cpi_dict)  
    
        ##using 270.97 for 2021 dollars
        df[f"adjusted_{col}"] = ((df[col] * current_year_dollars) / multiplier)
    return df

#Flag BlackCat only organizations as 1
def blackcat_only(row):
    #If there are no values for GTFS, reporter type, and fleet size, then we can probably say
    #this organization is not registered by Cal ITP or NTD: code them as 1. 
    if ((row.GTFS == 'None') and (row.reporter_type == 'None') and (row.fleet_size == 'No Info')):
        return "1"
    else:
        return "0"
    
#Determine if an agency has a small, medium, or large fleet size.
def fleet_size_rating(df): 
    #First grabbing only one row for each agency into a new data frame 
    Fleet_size = df.groupby(['organization_name',]).agg({'total_vehicles':'max'}).reindex()
    #Get percentiles in objects for total vehicle.
    p75 = df.total_vehicles.quantile(0.75).astype(float)
    p25 = df.total_vehicles.quantile(0.25).astype(float)
    p50 = df.total_vehicles.quantile(0.50).astype(float)
    #Function for fleet size
    def fleet_size (row):
        if ((row.total_vehicles > 0) and (row.total_vehicles <= p25)):
            return "Small"
        elif ((row.total_vehicles > p25) and (row.total_vehicles <= p75)):
            return "Medium"
        elif (row.total_vehicles > p75):
               return "Large"
        else:
            return "No Info"
    df["fleet_size"] = df.apply(lambda x: fleet_size(x), axis=1)
  
    return df    

"""
Cleaning up NTD Vehicles Data Set
"""
#Categorize vehicles down to 6 major categories
def get_vehicle_groups(row):
    Automobiles = ['Automobile','Sports Utility Vehicle']
    Bus = ['Bus','Over-the-road Bus','Articulated Bus',
           'Double Decker Bus','Trolleybus']
    Vans = ['Van','','Minivan','Cutaway']
    Trains = ['Vintage Trolley','Automated Guideway Vehicle',
              'Heavy Rail Passenger Car','Light Rail Vehicle',
             'Commuter Rail Self-Propelled Passenger Car','Commuter Rail Passenger Coach',
            'Commuter Rail Locomotive',  'Cable Car']
    Service = ['Automobiles (Service)',
               'Trucks and other Rubber Tire Vehicles (Service)',
               'Steel Wheel Vehicles (Service)']
    other = ['Other','Ferryboat']
    
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
    
#Initially cleaning vehicles dataset
def initial_cleaning(df):    
    #Add up columns 0-9 to get a new bin
    zero_to_nine = [0,1,2,3,4,5,6,7,8,9]
    ten_to_twelve = [10, 11, 12]
    
    df['0-9'] = df[zero_to_nine].sum(axis=1)
    #Add up columns 10-12
    df['10-12'] = df[ten_to_twelve].sum(axis=1)
    
    ## TO FIX
    # Method chaining, basically stringing or chaining together a bunch of commands
    # so it's a bit neater, and also it does it in one go
    df2 = df.drop(columns = zero_to_nine + ten_to_twelve)
    df2 = (to_snakecase(df2)
           .astype({"ntd_id": str}) 
           .rename(columns = {"_60+": "_60plus"})
          )
    
    df2["vehicle_groups"] = df2.apply(lambda x: get_vehicle_groups(x), axis=1)
    
    return df2

#Add up ages of the vehicles by agency
age_under_15 = ["_0_9","_10_12", "_13_15"]
age_over_15 = ["_16_20", "_21_25","_26_30", "_31_60","_60plus"]

def get_age(df):
    # Moved this renaming into initial_cleaning function
    #df = df.rename(columns={'_60+': '_60plus'})

    age = geography_utils.aggregate_by_geography(
        df, 
        group_cols = ["agency", "ntd_id", "reporter_type"],
        sum_cols = ["total_vehicles"] + age_under_15 + age_over_15,
        mean_cols = ["average_age_of_fleet__in_years_", "average_lifetime_miles_per_vehicle"]
    ).sort_values(["agency","total_vehicles"], ascending=[True, True])
    
    older = (age.query('_21_25 != 0 or _26_30 != 0 or _31_60 != 0 or _60plus!=0'))
    older = older.assign(
        sum_15plus = older[age_over_15].sum(axis=1)
    )
    
    age = pd.merge(age, 
                   older>>select(_.agency, _.sum_15plus), 
                   on=['agency'], how='left')
    return age

#Calculate # of doors by vehicle type 
def get_doors(df):
    
    types = df[["agency", "vehicle_groups"] + age_under_15 + age_over_15]
    types['sum_type'] = types[age_under_15 + age_over_15].sum(axis=1)
    
    ## At this point, the df is long (agency-vehicle_groups)
    
    #https://towardsdatascience.com/pandas-pivot-the-ultimate-guide-5c693e0771f3
    types2 = (types.pivot_table(index=["agency"],
                               columns="vehicle_groups", 
                               values="sum_type", aggfunc=np.sum, fill_value=0)
            ).reset_index()

    two_doors = ['Automobiles', 'Bus', 'Train']
    one_door = ['Van']
    door_cols = []
    
    for c in one_door + two_doors:
        # Create a new column, like automobile_door
        new_col = f"{c.lower()}_doors"
    
        # While new column is created, add it to list (door_cols)
        # Then, can easily sum across
        door_cols.append(new_col)
        
        if c in two_doors:
            multiplier = 2
        elif c in one_door:
            multiplier = 1
        types2[new_col] = types2[c] * multiplier
    
    types2["doors_sum"] = types2[door_cols].sum(axis=1)
    
    return types2  

#Get the completely clean data set
def clean_vehicles_data():
    vehicles = load_vehicle_data()
    vehicles2 = initial_cleaning(vehicles)

    # Use lists when there's the same set of columns you want to work with repeatedly
    # Break it up into several lists if need be
    # Whether lists live outside functions or inside functions depends if you need to call them again
    age_under_15 = ["_0_9","_10_12", "_13_15"]
    age_over_15 = ["_16_20", "_21_25","_26_30", "_31_60","_60plus"]
    
    # The lists above should live closer to the sub-functions they belong to
    
    # This df is aggregated at agency-level
    age_df = get_age(vehicles2)
    # This df is aggregated at agency-vehicle_group level 
    # but, pivoted to be agency-level
    doors_df = get_doors(vehicles2)
    
    df = pd.merge(
        age_df,
        doors_df,
        on = ["agency"],
        how = "left",
        validate = "1:1"
    )
    
    # Rename for now, because this might affect downstream stuff
    df = df.rename(columns = {"automobiles_doors": "automobiles_door"})    
    return df

"""
Merging 
in 3 steps 
"""
#Merge NTD Vehicles data set with GTFS info from Airtable
def ntd_airtable_merge():
    #Importing the dataframes: organizations is Cal ITP's Airtable
    #Vehicles is from NTD's vechicles dataset
    organizations = load_cleaned_organizations_data() 
    vehicles = clean_vehicles_data() 
    #merge the 2 datasets on the left, since there are many more entries on the left
    m1 = pd.merge(vehicles, organizations,  how='left', on='ntd_id')
    return m1


#Merge m1 with 5311 info from BlackCat
def ntd_airtable_5311_merge():
    m1 =  ntd_airtable_merge()
    df_5311 = load_grantprojects()
    #FIRST MERGE: 
    #5311 info from Black Cat on the left.
    m2 = (pd.merge(df_5311, m1,  how='left', left_on=['organization_name'], 
                      right_on=['name'], indicator=True)
            )
    #Some matches failed:subset out a df with left only matches
    Left_only = m2[(m2._merge.str.contains("left_only", case= False))] 
    
    #Take organizations left and make it into a list to filter out the df
    Left_orgs = Left_only['organization_name'].drop_duplicates().tolist()
    
    #Delete  left only matches from original df 
    m2 = m2[~m2.organization_name.isin(Left_orgs)]
    
    #SECOND MERGE:
    #Because we deleted organizations that were "left only",
    #make a data frame with these values & replace their names with ones in NTD data
    fail = df_5311[df_5311.organization_name.isin(Left_orgs)]
    
    #replacing organization names from Black Cat with agency names from m1  
    fail['organization_name'].replace(agency_crosswalk.crosswalk, inplace= True)
    
    #Merging the "failed" dataframe with m1 (NTD and GTFS Airtable info) 
    m3 = pd.merge(fail, m1,  how='left', left_on=['organization_name'], right_on=['agency'])
    
    #Concat: 
    m4 = pd.concat([m2, m3])
    return m4

#The final dataframe without any aggregation 
#Merge again with another airtable source
def final_df():
    df1 = ntd_airtable_5311_merge()
    airtable2 = load_airtable()
  
    #Change ITP ID ID to be floats & 0 so parquet will work
    df1['itp_id'] = (df1['itp_id']
                     .fillna(0)
                     .astype('int64')
                    )
 
    #Apply functions 
    #Call inflation function to add $ columns with adjusted values for inflation 
    df2 = adjust_prices(df1)
    #Apply fleet size() function
    df2 = fleet_size_rating(df2)
    
    #Merge df2 with the new airtable stuff 
    final = pd.merge(df2, airtable2, how='left', on='name')
    
    #Concatenate the two GTFS cols together into one column, to get complete GTFS status
    final["GTFS"] = final["gtfs_static_status"] + '_' + final["gtfs_realtime_status"]
    
    #Drop columns
    final = final.drop(columns = ['gtfs_static_status','gtfs_realtime_status','_merge', 'agency'])
    
    #Fill NA by data types
    final = final.fillna(final.dtypes.replace({'float64': 0.0, 'object': 'None'}))
   
    #Apply Black Cat only function 
    final["Is_Agency_In_BC_Only_1_means_Yes"] = final.apply(lambda x: blackcat_only(x), axis=1)
    
    #Manually replace Klamath
    #Klamath does not appear in NTD data so we missed it when we merged NTD & Cal ITP on NTD ID
    final.loc[(final['organization_name'] == 'Klamath Trinity Non-Emergency Transportation\u200b'), "GTFS"] = "Static OK_RT Incomplete"
    final.loc[(final['organization_name'] == 'Klamath Trinity Non-Emergency Transportation\u200b'), "itp_id"] = "436"
    
    return final

'''
Summarizing the Dataframe
'''
#Sums up original df to one row for each organization with aggregated statistics. 
#Takes the df from 700+ rows to <100 with info such as: total amount an organization 
#received, total vehicles, caltrans district, average fleet age, GTFS status.

#Columns for summing 
sum_cols = ['allocationamount',
'encumbered_amount',
'expendedamount', 'activebalance','closedoutbalance',
'adjusted_allocationamount', 'adjusted_expendedamount',
'adjusted_encumbered_amount', 'adjusted_activebalance']

#Columns for max 
max_cols = ['Is_Agency_In_BC_Only_1_means_Yes',
'total_vehicles',
'average_age_of_fleet__in_years_',
'average_lifetime_miles_per_vehicle',
'Automobiles', 'Bus','Other', 'Train',
'Van','automobiles_door',
'bus_doors', 'van_doors', 'train_doors', 
'doors_sum','_31_60', '_16plus','_60plus', 
'adjusted_closedoutbalance']

#Columns for mean
mean_cols = ['allocation_mean']           

#Function for aggregating   
def aggregated_df(): 
    #Read original df & airtable
    df = final_df()
    
    #Sum all vehicles 16+ above into one col 
    column_names = ['_16_20', '_21_25', '_26_30', '_31_60']
    df['_16plus']= df[column_names].sum(axis=1)
    
    #Duplicate allocationamount col so we can get the mean an organiztaion received.
    df['allocation_mean'] = df['allocationamount']
    
    #Aggregate
    #https://stackoverflow.com/questions/67717440/use-a-list-of-column-names-in-groupby-agg
    df = ((df.groupby(['organization_name',
                     'reporter_type', 
                     'fleet_size', 
                     'ntd_id', 
                     'itp_id','mpo_rtpa',
                      'GTFS','caltrans_district',
                      'planning_authority'], as_index=False)
                    .agg({**{e:'max' for e in max_cols}, 
                          **{e:'sum' for e in sum_cols}, 
                          **{e: 'mean' for e in mean_cols}})
                   .reset_index())
         ) 
         
   
    return df 
    