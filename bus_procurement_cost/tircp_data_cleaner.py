import matplotlib.pyplot as plt
import pandas as pd
import shared_utils

gcs_path = 'gs://calitp-analytics-data/data-analyses/bus_procurement_cost/'
file_name = 'TIRCP Tracking Sheets 2_1-10-2024.xlsx'
sheet_name = 'Project Tracking'

def get_data(path, file, sheet):
    '''
    read in tircp data from project tracking tab
    '''
    df = pd.read_excel(path+file, sheet_name=sheet)
    
    return df

project = get_data(gcs_path, file_name, sheet_name)

# only keep first couple of columns
project = project.iloc[:, :20]

# dropping specific columns
drop_col = [
    "Master Agreement Expiration Date",
    "Project Manager",
    "Regional Coordinator",
    "Technical Assistance-CALITP (Y/N)",
    "Technical Assistance-Fleet (Y/N)",
    "Technical Assistance-Network Integration (Y/N)",
    "Technical Assistance-Priority Population (Y/N)",
    "Allocated Amount",
]

project.drop(columns=drop_col, inplace=True)

# replace columns space with _ & lower everything
project.columns = project.columns.str.replace(" ", "_")
project.columns = project.columns.str.lower()

#clean `grant_recipient column
agency_dict = {
    "Antelope Valley Transit Authority ": "Antelope Valley Transit Authority (AVTA)",
    "Humboldt Transit Authority": "Humboldt Transit Authority (HTA)",
    "Orange County Transportation Authority": "Orange County Transportation Authority (OCTA)",
    "Capitol Corridor Joint Powers Authority": "Capitol Corridor Joint Powers Authority (CCJPA)",
    "Los Angeles County Metropolitan Transportation Authority": "Los Angeles County Metropolitan Transportation Authority (LA Metro)",
    "Monterey-Salinas Transit": "Monterey-Salinas Transit District (MST)",
    "Sacramento Regional Transit (SacRT)": "Sacramento Regional Transit District (SacRT)",
    "Sacramento Regional Transit District": "Sacramento Regional Transit District (SacRT)",
    "Sacramento Regional Transit District (SacRT) ": "Sacramento Regional Transit District (SacRT)",
    "San Diego Association of Governments": "San Diego Association of Governments (SANDAG)",
    "Santa Clara Valley Transportation Authority (SCVTA)": "Santa Clara Valley Transportation Authority (VTA)",
    "Southern California  Regional Rail Authority (SCRRA)": "Southern California Regional Rail Authority (SCRRA - Metrolink)",
    "Southern California Regional Rail Authority": "Southern California Regional Rail Authority (SCRRA - Metrolink)",
}

project.replace({"grant_recipient": agency_dict}, inplace=True)


# clean `county` column
# change county value from '3, 4' to 'VAR' like the other rows.
project.at[3, "county"] = "VAR"

# updating bus count value for certain row 
project.loc[project['ppno'] == 'CP106', 'bus_count'] = 42

#filtering project to only have bus_count >0
bus_only = project[project["bus_count"] > 0]

#preparing for new `prop_type` column
prop_type = [
    "electric buses",
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
]

def prop_type_finder(description):
    '''
    function for finding prop type from project description column, and returning the keyword it matched on
    '''
    for keyword in prop_type:
        if keyword in description:
            return keyword
    return "not specified"

bus_only["prop_type"] = bus_only["project_description"].apply(prop_type_finder)

#dictionary to consolidate/validate prop_types
prop_dict = {
    "battery electric": "BEB",
    "battery-electric": "BEB",
    "electric buses": "electric (not specified)",
    "electric commuter": "electric (not specified)",
    "fuel cell": "FCEB",
    "fuel-cell": "FCEB",
    "zero-emission buses": "zero-emission bus (not specified)",
    "zero emission": "zero-emission bus (not specified)",
    "zero-emission": "zero-emission bus (not specified)",
    "zero‐emission": "zero-emission bus (not specified)",
}

bus_only.replace({"prop_type": prop_dict}, inplace=True)

#prepare for new column `bus_size_type`
bus_size = [
    "standard",
    "30-foot",
    "40 foot",
    "40-foot",
    "45-foot",
    "45 foot",
    "40ft",
    "60-foot",
    "articulated",
    "cutaway",
    "coach-style",
    "over-the-road",
    "feeder bus",
]

# re writing prop type funct for bus size
def bus_size_finder(description):
    '''
    similar to prop_type function. returns bus size keyword that matched in the project description col'''
    for keyword in bus_size:
        if keyword in description:
            return keyword
    return "not specified"

bus_only["bus_size_type"] = bus_only["project_description"].apply(bus_size_finder)

#dictionary to consolidate bus sizes

size_dict={'40 foot': 'conventional (40-ft like)' ,
 '40-foot': 'conventional (40-ft like)',
 '45-foot': 'conventional (40-ft like)',
 'coach-style':'over-the-road',
 'feeder bus': 'conventional (40-ft like)',
 }

bus_only.replace({"bus_size_type": size_dict}, inplace=True)

#export as parquet
bus_only.to_parquet(
    "gs://calitp-analytics-data/data-analyses/bus_procurement_cost/tircp_project_bus_only.parquet"
)