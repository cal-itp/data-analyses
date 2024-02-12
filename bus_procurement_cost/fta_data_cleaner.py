import numpy as np
import pandas as pd
import shared_utils

#read in fta data
gcs_path = "gs://calitp-analytics-data/data-analyses/bus_procurement_cost/"
file = "data-analyses_bus_procurement_cost_fta_press_release_data_csv.csv"

fta = pd.read_csv(gcs_path+file)

# functions
def snake_case(df):
    '''
    snake case dataframe columns and stip of extra spaces
    '''
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.strip()


def fund_cleaner(df, column):
    '''
    function to clean the funding column and make column int64
    '''
    df[column] = df[column].str.replace("$", "").str.replace(",", "").str.strip().astype('int64')


def value_replacer(df, col1, col1_val, col2, col2_new_val):
    '''
    function that replaces the value at a speicific row on a specific column.
    in this case, filters the df by a speific col/val, then replaces the value at new col/val
    '''
    df.loc[df[col1] == col1_val , col2] = col2_new_val


snake_case(fta)

# rename col to propulsion category
fta = fta.rename(columns={"propulsion_type": "propulsion_category"})

# make values in prop_cat col lower case and remove spaces
fta["propulsion_category"] = fta["propulsion_category"].str.lower()
fta["propulsion_category"] = fta["propulsion_category"].str.replace(" ", "")


fund_cleaner(fta, "funding")

# removing the spaces first in # of bus colum values, THEN split by (
fta["approx_#_of_buses"] = fta["approx_#_of_buses"].str.replace(" ", "")

# spliting the # of buses column into 2, using the ( char as the delimiter
# also fills `none` values with `needs manual check`
fta[["bus_count", "prop_type"]] = fta["approx_#_of_buses"].str.split(
    pat="(", n=1, expand=True
)
fta[["bus_count", "prop_type"]] = fta[["bus_count", "prop_type"]].fillna(
    "needs manual check"
)

# running function on rows that need specific value changes
value_replacer(fta,'bus_count','56estimated-cutawayvans', 'bus_count', 56)
value_replacer(fta,'bus_count','12batteryelectric','bus_count', 12)
value_replacer(fta,'prop_type','PM-awardwillnotfund68buses)', 'prop_type', 'estimated-cutaway vans (PM- award will not fund 68 buses)')
value_replacer(fta,'project_sponsor','City of Charlotte - Charlotte Area Transit System','bus_count',31)


# using str.lower() on project type
fta["project_type"] = fta["project_type"].str.lower().str.replace(" ", "")

# some values still need to get adjusted. will use a short dictionary to fix
new_type = {
    "\tbus/facility": "bus/facility",
    "bus/facilitiy": "bus/facility",
    "facilities": "facility",
}
# using replace() with the dictionary to replace keys in project type col
fta.replace({"project_type": new_type}, inplace=True)

# clearning the bus desc/prop_type col.
# removing the )
fta["prop_type"] = fta["prop_type"].str.replace(")", "").str.strip()

# creating a dictionary to add spaces back to the values
spaces = {
    "beb": "BEB",
    "estimated-CNGbuses": "estimated-CNG buses",
    "cngbuses": "CNG buses",
    "BEBs": "BEB",
    "Electric\n16(Hybrid": "15 electic, 16 hybrid",
    "FuelCellElectric": "fuel cell electric",
    "FuelCell": "fuel cell",
    "lowemissionCNG": "low emission CNG",
    "cng": "CNG",
    "BEBsparatransitbuses": "BEBs paratransit buses",
    "hybridelectric": "hybrid electric",
    "zeroemissionbuses": "zero emission buses",
    "dieselelectrichybrids": "diesel electric hybrids",
    "hydrogenfuelcell": "hydrogen fuel cell",
    "2BEBsand4HydrogenFuelCellBuses": "2 BEBs and 4 hydrogen fuel cell buses",
    "4fuelcell/3CNG": "4 fuel cell / 3 CNG",
    "hybridelectricbuses": "hybrid electric buses",
    "CNGfueled": "CNG fueled",
    "zeroemissionelectric": "zero emission electric",
    "hybridelectrics": "hybrid electrics",
    "dieselandgas": "diesel and gas",
    "diesel-electrichybrids": "diesel-electric hybrids",
    "propanebuses": "propane buses",
    "1:CNGbus;2cutawayCNGbuses": "1:CNGbus ;2 cutaway CNG buses",
    "zeroemission": "zero emission",
    "propanedpoweredvehicles": "propaned powered vehicles",
}

# using new dictionary to replace values in the bus desc col
fta.replace({"prop_type": spaces}, inplace=True)

# dict to validate prop_type values
prop_type_dict = {
    "15 electic, 16 hybrid": "mix (zero and low emission buses)",
    "1:CNGbus ;2 cutaway CNG buses": "mix (zero and low emission buses)",
    "2 BEBs and 4 hydrogen fuel cell buses": "mix (BEB and FCEB)",
    "4 fuel cell / 3 CNG": "mix (zero and low emission buses)",
    "BEBs paratransit buses": "BEB",
    "CNG buses": "CNG",
    "CNG fueled": "CNG",
    "Electric": "electric (not specified)",
    "battery electric": "BEB",
    "diesel and gas": "mix (low emission)",
    "diesel electric hybrids": "low emission (hybrid)",
    "diesel-electric": "low emission (hybrid)",
    "diesel-electric hybrids": "low emission (hybrid)",
    "electric": "electric (not specified)",
    "estimated-CNG buses": "CNG",
    "estimated-cutaway vans (PM- award will not fund 68 buses": "mix (zero and low emission buses)",
    "fuel cell": "FCEB",
    "fuel cell electric": "FCEB",
    "hybrid": "low emission (hybrid)",
    "hybrid electric": "low emission (hybrid)",
    "hybrid electric buses": "low emission (hybrid)",
    "hybrid electrics": "low emission (hybrid)",
    "hydrogen fuel cell": "FCEB",
    "low emission CNG": "CNG",
    "propane": "low emission (propane)",
    "propane buses": "low emission (propane)",
    "propaned powered vehicles": "low emission (propane)",
    "zero emission": "zero-emission bus (not specified)",
    "zero emission buses": "zero-emission bus (not specified)",
    "zero emission electric": "zero-emission bus (not specified)",
    "zero-emission": "zero-emission bus (not specified)",
}

# repalcing values in prop type with prop type dictionary
fta.replace({"prop_type": prop_type_dict}, inplace=True)

# subdf of just `needs manual check` prop_types
manual_check = fta[fta["prop_type"] == "needs manual check"]

# function to match keywords to list
def prop_type_finder(description):
    '''
    to be used with .apply() against description col
    '''
    for keyword in manual_checker_list:
        if keyword in description:
            return keyword
    return "no bus procurement"

manual_checker_list = [
    "propane-powered",
    "hybrid diesel-electric buses",
    "propane fueled buses",
    "cutaway vehicles",
    "diesel-electric hybrid",
    "low or no emission buses",
    "electric buses",
    "hybrid-electric vehicles",
    "electric commuter",
    "Electric Buses",
    "battery electric",
    "Batery Electric",
    "battery-electric",
    "fuel-cell",
    "fuel cell",
    "Fuel Cell",
    "zero emission",
    "Zero Emission",
    "zero-emission electric buses",
    "zero-emission buses",
    "zero‐emission",
    "zero-emission",
    "zeroemission",
    "CNG",
    "cng",
    "County Mass Transit District will receive funding to buy buses",
    "Colorado will receive funding to buy vans to replace older ones",
    "ethanol-fueled buses",
    "will receive funding to buy vans to replace",
    "funding to replace the oldest buses",
    "to buy buses and charging equipment",
    "counties by buying buses",
    "receive funding to buy cutaway paratransit buses",
    "new replacement vehicles",
]

# creates a new column called 'prop_type' by applying function to description column. 
# the function will check the values against the description col against the list, then return the keyword the row matched too
manual_check["prop_type"] = manual_check["description"].apply(prop_type_finder)

# dictionary to change manual check prop_type to validated values
manual_check_dict= {'zero emission': 'zero-emission bus (not specified)',
 'electric buses':'electric (not specified)',
 'zero-emission': 'zero-emission bus (not specified)',
 'low or no emission buses' : 'mix (zero and low emission buses)',
 'zero-emission buses': 'zero-emission bus (not specified)',
 'new replacement vehicles':'not specified',
 'receive funding to buy cutaway paratransit buses': 'not specified',
 'counties by buying buses': 'not specified',
 'battery-electric' : 'BEB',
 'to buy buses and charging equipment':'not specified',
 'propane-powered': 'low emission (propane)',
 'funding to replace the oldest buses':'not specified',
 'diesel-electric hybrid': 'low emission (hybrid)',
 'hybrid diesel-electric buses': 'low emission (hybrid)',
 'cutaway vehicles':'not specified',
 'propane fueled buses': 'low emission (propane)',
 'County Mass Transit District will receive funding to buy buses':'not specified',
 'ethanol-fueled buses': 'low emission (ethanol)',
 'will receive funding to buy vans to replace': 'not specified',
 'Colorado will receive funding to buy vans to replace older ones': 'not specified',
 'hybrid-electric vehicles': 'low emission (hybrid)'
}

# replace prop_type values using manual_check_dict
manual_check.replace({"prop_type": manual_check_dict}, inplace=True)

# filters df for rows that do not equal `needs manual check`
# expect rows to drop from 130 to 72?
fta = fta[fta['prop_type'] != 'needs manual check']

# appending rows from manual_check to initial df
fta = fta.append(manual_check, ignore_index=True)

# new column for bus_size_type
bus_size = [
    "standard",
    "40 foot",
    "40-foot",
    "40ft",
    "articulated",
    "cutaway",
]

# Function to match keywords
def find_bus_size_type(description):
    '''similar to prop_type func. use with .apply() to description to get bus size'''
    for keyword in bus_size:
        if keyword in description.lower():
            return keyword
    return "not specified"

# new column called bus size type based on description column
fta["bus_size_type"] = fta["description"].apply(find_bus_size_type)


# saving to GCS as csv

clean_file = 'fta_bus_cost_clean.csv'

fta.to_csv(gcs_path+clean_file)