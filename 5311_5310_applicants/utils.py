import pandas as pd
import numpy as np
from calitp import *
from siuba import *
from shared_utils import geography_utils


# Clean organization names 
def organization_cleaning(df, column_wanted: str):
    df[column_wanted] = (
        df[column_wanted]
        .str.strip()
        .str.split(",")
        .str[0]
        .str.replace("/", "")
        .str.split("(")
        .str[0]
        .str.split("/")
        .str[0]
    )
    return df

'''
Puts all the elements in the column "col to summarize" onto one line and separates them by commas. 
For ex: an agency typically purchases 1+ products but in the original dataset,
each product has its own line: this function can grab all the products by agency and puts them on the same line.
'''
def summarize_rows(df, col_to_group: str, col_to_summarize: str):
    df_col_to_summarize = (df
    .groupby(col_to_group)[col_to_summarize]
    .apply(','.join)
    .reset_index()
     )
    return df_col_to_summarize

# Remove underscores, title case, and strip whitespaces
def clean_up_columns(df):
    df.columns = df.columns.str.replace("_", " ").str.title().str.strip()
    return df

# Function that moves all elements in a particular column that is separated by commas onto one line 
def service_comps_summarize(df, components_wanted: list):

    # Filter out
    df = df[df["service_components_component_name"].isin(components_wanted)]

    # Puts all the components  an organization purchases onto one line, instead of a few different ones into a df
    comps = summarize_rows(
        df, "service_components_service_name", "service_components_component_name"
    )

    # Puts all product names onto one line into a new df
    prods = summarize_rows(
        df, "service_components_service_name", "service_components_product_name"
    )

    # Merge comps and prods together
    m1 = pd.merge(comps, prods, how="inner", on="service_components_service_name")

    return m1


"""
Cleaning up NTD Vehicles Data Set
from 5311 work https://github.com/cal-itp/data-analyses/blob/main/5311/_data_prep.py
"""
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/5311 /"

#Vehicles data from NTD
def load_vehicle_data():
    File_Vehicles =  "cleaned_vehicles.xlsx"
    vehicles_info =  pd.read_excel(f'{GCS_FILE_PATH}{File_Vehicles}',
                                   sheet_name = 'Age Distribution')
    vehicles = (vehicles_info>>filter(_.State=='CA'))
    
    return vehicles

#Categorize vehicles down to 6 major categories
def get_vehicle_groups(row):
    Automobiles = ["Automobile", "Sports Utility Vehicle"]
    Bus = ["Bus", "Over-the-road Bus", "Articulated Bus", "Double Decker Bus", "Trolleybus"]
    Vans = ["Van", "", "Minivan", "Cutaway"]
    Trains = [
    "Vintage Trolley",
    "Automated Guideway Vehicle",
    "Heavy Rail Passenger Car",
    "Light Rail Vehicle",
    "Commuter Rail Self-Propelled Passenger Car",
    "Commuter Rail Passenger Coach",
    "Commuter Rail Locomotive",
    "Cable Car",
    ]
    Service = [
    "Automobiles (Service)",
    "Trucks and other Rubber Tire Vehicles (Service)",
    "Steel Wheel Vehicles (Service)",
    ]
    other = ["Other", "Ferryboat"]
    
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