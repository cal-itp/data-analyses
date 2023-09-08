'''
Utils for IIJA Project Descriptions and Information:

Functions here to be used in cleaning script that will 
- add known names to organizations
- add project types that classify project type
- create a public-friendly project title 
'''

import numpy as np
import pandas as pd
from siuba import *

import dla_utils

from calitp_data_analysis.sql import to_snakecase

import _data_utils

import intake

# import nltk
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize, sent_tokenize

# import re

GCS_FILE_PATH  = 'gs://calitp-analytics-data/data-analyses/dla/dla-iija'


#get column names in Title Format (for exporting)
def title_column_names(df):
    df.columns = df.columns.map(str.title) 
    df.columns = df.columns.map(lambda x : x.replace("_", " "))
    
    return df


def change_district_format(df, district_col):
    """
    Function to reformat the district columns
    """
    district_map = ({'1.0':'01', '2.0':'02', '3.0':'03',
                 '4.0': '04', '5.0': '05', '6.0':'06', '7.0':'07',
                 '8.0':'08', '9.0':'09', '10.0':'10',
                 '11.0':'11', '12.0':'12'})
        
    df[district_col] = df[district_col].astype(str)
        
    df[district_col] = df[district_col].map(district_map)
                
    return df

def format_congressional_district(df, con_dist_col):
    """
    reformat the congressional district column
    """
    
    df = _data_utils.change_col_to_integer(df, "congressional_district")
    
    df = df.replace({'0': '01', '1': '01', '2':'02', '3':'03', '4':'04',
               '5':'05', '6':'06', '7':'07', '8':'08', '9':'09'})
    
    return df
    

def add_county_abbrev(df, county_name_col):
    '''
    Function to add county abbreviation to a county name EX: 'Kings County' to 'KIN'
    Note: if your county name column has 'County' already in the string, then hash out line 3 of the function
    that adds it in for counties with no 'County' in the name
    '''
    ### read county data in from the shared_data catalog
    catalog = intake.open_catalog("../../_shared_utils/shared_utils/shared_data_catalog.yml")
    counties = to_snakecase((catalog.ca_counties.read())>>select(_.COUNTY_NAME,_.COUNTY_ABBREV,))
    
    county_codes = to_snakecase((pd.read_excel(f"{GCS_FILE_PATH}/CountyNameToCodeLookUp.xlsx")))
    
    ### add county 
    counties['county_name_full'] = counties['county_name'] + ' County'
    
    ### create dict to map
    county_mapping = dict(county_codes[['county_name', 'rca_county_code']].values)
    
    ### map values to the df
    df[f"{county_name_col}_abbrev"] = df[county_name_col].map(county_mapping)
    
    df[f"{county_name_col}_abbrev"] = df[f"{county_name_col}_abbrev"].fillna('NA')
    
    return df

#add project information for all projects
def identify_agency(df, identifier_col):
    #projects wtih locodes
    locode_proj = ((df[~df[identifier_col].str.contains(" ")]))
    locode_proj = locode_proj>>filter(_[identifier_col]!='None')
    
    locode_proj = (_data_utils.add_name_from_locode(locode_proj, 'summary_recipient_defined_text_field_1_value'))
    

    #projects with no locodes
    no_locode = ((df[df[identifier_col].str.contains(" ")]))
    no_entry = df>>filter(_[identifier_col]=='None')
    
    #concat no locodes and those with no entry
    no_locode = pd.concat([no_locode, no_entry])
    
    #add county codes:
    county_base = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/Copy of County.xlsx", sheet_name='County', header=[1]))
    county_base.drop(columns =['unnamed:_0', 'unnamed:_4'], axis=1, inplace=True)
    county_base['county_description'] = county_base['county_description'] + " County"
    
    #read in locode info
    locodes = to_snakecase(pd.read_excel(f"gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/locodes_updated7122021.xlsx"))
    county_district = locodes>>group_by(_.district, _.county_name)>>count(_.county_name)>>select(_.district, _.county_name)>>filter(_.county_name!='Multi-County', _.district !=53)
    
    # merge county information to add districts
    county_info = (pd.merge(county_base, county_district, how='left', left_on= 'county_description', right_on = 'county_name'))
    county_info.drop(columns =['county_name'], axis=1, inplace=True)
    
    mapping1 = dict(county_info[['county_code', 'county_description']].values)
    mapping2 = dict(county_info[['county_code', 'recipient_name']].values)
    mapping3 = dict(county_info[['county_code', 'district']].values)
    
    no_locode['county_description'] = no_locode.county_code.map(mapping1)
    no_locode['recipient_name'] = no_locode.county_code.map(mapping2)
    no_locode['district'] = no_locode.county_code.map(mapping3)
    
    no_locode = no_locode.rename(columns = {"recipient_name":"implementing_agency", "county_description":"county_name"})

    full_df = pd.concat([locode_proj, no_locode])
    
    full_df.loc[full_df.county_name == "Statewide County", 'county_name'] = "Statewide"
    
    full_df['implementing_agency'] = full_df['implementing_agency'].fillna(value='Unknown')
    full_df['county_name'] = full_df['county_name'].fillna(value='Unknown')

    
    return full_df




def condense_df(df):
    """
    Function to return one row for each project and keep valuable unique information for the project
    """
    # make sure columns are in string format
    df[['county_code', 'improvement_type',
     'implementing_agency_locode', 'district',
     'program_code_description', 'recipient_project_number']] = df[['county_code', 'improvement_type',
                                                                     'implementing_agency_locode', 'district',
                                                                     'program_code_description', 'recipient_project_number']].astype(str)
    # copy county column over to use for project title name easier
    df['county_name_title'] = df['county_name'] 
    
    # aggreate df using .agg function and join in the unique values into one row
    df_agg = (df
           .assign(count=1)
           .groupby(['fmis_transaction_date','project_number', 'implementing_agency', 'summary_recipient_defined_text_field_1_value'
                    # , 'program_code', 'program_code_description'
                    ])
           .agg({
                 # 'program_code':lambda x:'|'.join(x.unique()), # get unique values to concatenate                ##hashing this out to group by instead
                 # 'program_code_description':lambda x:'|'.join(x.unique()), # get unique values to concatenate    ##hashing this out to group by instead
                 'recipient_project_number':lambda x:'|'.join(x.unique()), #'first',
                 'improvement_type':lambda x:'|'.join(x.unique()), # get unique values to concatenate
                 'improvement_type_description':lambda x:'|'.join(x.unique()),  # get unique values to concatenate
                 'program_code':lambda x:'|'.join(x.unique()),  # get unique values to concatenate
                 'program_code_description':lambda x:'|'.join(x.unique()),  # get unique values to concatenate
                 'project_title':'first', #should be the same                 
                 'obligations_amount':'sum', #sum of the obligations amount
                 'congressional_district':lambda x:'|'.join(x.unique()), # get unique values to concatenate
                 'district':lambda x:'|'.join(x.unique()), # get unique values to concatenate
                 'county_code':lambda x:'|'.join(x.unique()), # get unique values to concatenate
                 'county_name':lambda x:'|'.join(x.unique()), # get unique values to concatenate
                 'county_name_abbrev':lambda x:'|'.join(x.unique()), # get unique values to concatenate 
                 'county_name_title':lambda x:' & '.join(x.unique()), # get unique values to concatenate
                 'implementing_agency_locode':lambda x:'|'.join(x.unique()), # get unique values to concatenate
                 'rtpa_name':'first', #should be the same
                 'mpo_name':'first',  #should be the same
                }).reset_index())
    
    df_agg['obligations_amount'] = df_agg['obligations_amount'].astype(np.int64)
    
    df_agg['district'] = '|' + df_agg['district'] + '|'
    df_agg['congressional_district'] = '|' + df_agg['congressional_district'] + '|'
    df_agg['county_name_abbrev'] = '|' + df_agg['county_name_abbrev'] + '|'
    
    return df_agg

def add_description(df, col):
    ##using np.where. code help: https://stackoverflow.com/questions/43905930/conditional-if-statement-if-value-in-row-contains-string-set-another-column
    
    ## make sure column is in ALL CAPS
    df[col] = df[col].str.upper()
    
    ## method for project in first column
    df['project_method'] = (np.where(df[col].str.contains("INSTALL"), "Install",
                        np.where(df[col].str.contains("RECONSTRUCT"), "Reconstruct",
                        np.where(df[col].str.contains("CONSTRUCT") & df[col].str.contains("PERMANENT RESTORATION") , "",
                        np.where(df[col].str.contains("CONSTRUCT"), "Construct",
                        np.where(df[col].str.contains("UPGRADE"), "Upgrade",
                        np.where(df[col].str.contains("IMPROVE"), "Improve",
                        np.where(df[col].str.contains("ADD "), "Add",
                        np.where(df[col].str.contains("REPAIR"), "Repair",
                        np.where(df[col].str.contains("REPLACE"), "Replace",
                        np.where(df[col].str.contains("REPLACE ")& df[col].str.contains("BRIDGE"), "",
                        np.where(df[col].str.contains("REPLACE")& df[col].str.contains("GUARDRAIL"), "Replace",
                        np.where(df[col].str.contains("REPAVE")| df[col].str.contains("REPAVING"), "Repave",
                        np.where(df[col].str.contains("NEW "), "New",
                        np.where(df[col].str.contains("EXTEND"), "Extend",
                        np.where(df[col].str.contains("IMPLEMENT"), "Implement",
                        np.where(df[col].str.contains("RESTORATION") & df[col].str.contains("PERMANENT RESTORATION") , "",
                        np.where(df[col].str.contains("RESTORATION"), "Restoration",
                        
                                    ""))))))))))))))))))
    
    ## types of projects in second column
    df['project_type'] = (
                        #np.where(df.col.str.contains("BRIDGE REPLACEMENT") , "Bridge Replacement",
                        np.where(df[col].str.contains("SHOULDER") & df[col].str.contains("RESTORE") | df[col].str.contains("RESTORATON"), "Restore Shoulders",
                        np.where(df[col].str.contains("WIDEN SHOULDER"), "Widen Shoulders",
                        np.where(df[col].str.contains("SHOULDER"), "Shoulders",
                        np.where(df[col].str.contains("RESTORE ROADWAY"),"Road Restoration & Rehabilitation", 
                        np.where(df[col].str.contains("SYNCHRONIZE CORRIDOR"), "Synchronize Corridor",
                        np.where(df[col].str.contains("COMPLETE STREET"), "Complete Streets",
                        np.where(df[col].str.contains("BRIDGE PREVENTIVE MAINTENANCE"), "Bridge Preventive Maintenance",
                        np.where(df[col].str.contains("SIDEWALK"), "Sidewalk",
                        np.where(df[col].str.contains("SCOUR"), "Erosion Countermeasures",
                        np.where(df[col].str.contains("ROUNDABOUT") | df[col].str.contains("ROUDABOUT"), "Roundabout",
                        np.where(df[col].str.contains("TURN LANE"), "Turn Lane",
                        np.where(df[col].str.contains("GUARDRAI"), "Guardrails", ##removing the "L"from Guardrail in case the word is cut off
                        np.where(df[col].str.contains("VIDEO DETECTION EQUIPMENT"), "Video Detection Equipment",
                        np.where(df[col].str.contains("PEDESTRIAN") & df[col].str.contains("BIKE") , "Pedestrian & Bike Safety Improvements",
                        np.where(df[col].str.contains("CONSTRUCT HOV"), "HOV Lane",
                        np.where(df[col].str.contains("CONVERT EXISTING HOV LANES TO EXPRESS LANES"), "Convert HOV Lanes to Express Lanes",    
                        np.where(df[col].str.contains("EXPRESS LANES"), "Express Lanes",         
                        np.where(df[col].str.contains("HOV") | df[col].str.contains("HIGH-OCCUPANCY LANE"), "HOV Lane", 
                        np.where(df[col].str.contains("BRIDGE") & df[col].str.contains("REHAB") , "Bridge Rehabilitation",
                        np.where(df[col].str.contains("PAVEMENT") & df[col].str.contains("REHAB") , "Pavement Rehabilitation",
                        np.where(df[col].str.contains("PEDESTRIAN"), "Pedestrian Safety Improvements",
                        np.where(df[col].str.contains("TRAFFIC SIG"), "Traffic Signals",
                        np.where(df[col].str.contains("BIKE SHARE"), "Bike Share Program",
                        np.where(df[col].str.contains("BIKE"), "Bike Lanes",                  
                        np.where(df[col].str.contains("SIGNAL"), "Signals",
                        np.where(df[col].str.contains("SIGN") & ~df[col].str.contains('DESIGN'), "Signage",
                        np.where(df[col].str.contains("BRIDGE REPLACEMENT") | df[col].str.contains("REPLACE EXISTING BRIDGE") | df[col].str.contains("REPLACE BRIDGE"), "Bridge",
                        np.where(df[col].str.contains("LIGHT"), "Lighting",         
                        np.where(df[col].str.contains("SAFETY ") & df[col].str.contains("IMPROVE") , "Safety Improvements",
                        np.where(df[col].str.contains("ROAD REHAB") | df[col].str.contains("ROADWAY REHAB"), "Road Rehabiliation",
                        np.where(df[col].str.contains("RAISED") & df[col].str.contains("MEDIAN"), "Raised Median",
                        np.where(df[col].str.contains("MEDIAN"), "Median",
                        np.where(df[col].str.contains("AUXILIARY LANE"), "Auxiliary Lane",
                        np.where(df[col].str.contains("TO EXPRESS LANES"), "Express Lanes",
                        np.where(df[col].str.contains("STORMWATER TRE"), "Storm Water Mitigation",
                        np.where(df[col].str.contains("WIDEN"), "Widen Road",
                        np.where(df[col].str.contains("REGIONAL PLANNING ACTIVITIES AND PLANNING, PROGRAMMING"), "Regional Planning Activities",
                        np.where(df[col].str.contains("SLIDE REPAIR"), "Slide Repair",  
                        np.where(df[col].str.contains("STABILIZE") & df[col].str.contains("EMBANKMENT"), "Stabilize Embankment", 
                        np.where(df[col].str.contains("EMBANKMENT RESTORATION") , "Restore Embankment", 
                        np.where(df[col].str.contains("EMBANKMENT RECONSTRUCTION") , "Reconstruct Embankment", 
                        np.where(df[col].str.contains("RAMP"), "Ramp",
                        np.where(df[col].str.contains("SEISMIC RETROFIT"), "Seismic Retrofit",        
                        np.where(df[col].str.contains("INTELLIGENT TRANSPORTATION SYSTEM"), "Intelligent Transportation Systems",         
                        np.where(df[col].str.contains("OC STRUCTURES"), "OC Structures",       # Maybe On-Center
                        np.where(df[col].str.contains("WITH 2-LANE BRIDGE") | df[col].str.contains("WITH 2 LANE BRIDGE") | df[col].str.contains("REPLACE EXISTING ONE LANE BRIDGE") | df[col].str.contains("REPLACE EXISTING ONE-LANE BRIDGE"),"Bridge",
                        np.where(df[col].str.contains("RESTORE WETLANDS") ,"Restore Wetlands",
                        np.where(df[col].str.contains("CLEAN AIR TRANSPORTATION PROGRAM") ,"Clean Air Transportation Program",
                        np.where(df[col].str.contains("STREETS AND ROADS PROGRAM") ,"Streets and Roads Program",
                        np.where(df[col].str.contains("MAPPING") ,"Mapping Project",
                     #   np.where(df[col].str.contains("VIADUCT") ,"Viaduct",
                        np.where(df[col].str.contains("OVERHEAD") ,"Overhead",         
                        np.where(df[col].str.contains("SHORELINE EMBANKMENT") ,"Shoreline Embankment", 
                        np.where(df[col].str.contains("NON-INFRAS") ,"Non-Infrastructure Project",
                        np.where(df[col].str.contains("PILOT PROGRAM") ,"Pilot Program", 
                       # np.where(df[col].str.contains("PLANNING") ,"Planning", 
                        np.where(df[col].str.contains("REC TRAILS") ,"Recreational Trails Project", 
                        np.where(df[col].str.contains("PLANT") & df[col].str.contains("IRRIGATION") ,"Planting and Irrigation Systems", 
                        np.where(df[col].str.contains("PLANT") & df[col].str.contains("VE") ,"Plant Vegetation",
                        np.where(df[col].str.contains("PERMANENT RESTORATION"),"Road Restoration & Rehabilitation",
                        np.where(df[col].str.contains("PLANNING GRANT"),"Planning and Research",
                        np.where(df[col].str.contains("PLANNING AND RESEARCH"),"Planning and Research",

                                 'Project')
                                   ))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))#)
    
    
    return df


def add_description_4_no_match(df, desc_col):
    ##using np.where. code help: https://stackoverflow.com/questions/43905930/conditional-if-statement-if-value-in-row-contains-string-set-another-column

    
    ## method for project in first column
    df['project_type2'] = (np.where(df[desc_col].str.contains("Bridge Rehabilitation"),"Bridge Rehabilitation",
                        np.where(df[desc_col].str.contains("Bridge Rehabilitation - No Added Capacity") | df[desc_col].str.contains("Bridge Rehabilitation - Added Capacity"), "Bridge Rehabilitation",
                        np.where(df[desc_col].str.contains("Bridge Replacement - Added Capacity")| df[desc_col].str.contains("Bridge Replacement - No Added Capacity"), "Bridge Replacement",
                        np.where(df[desc_col].str.contains("Bridge New Construction")| df[desc_col].str.contains("Special Bridge"), "Bridge Replacement",
                        np.where(df[desc_col].str.contains("Facilities for Pedestrians and Bicycles"), "Facilities for Pedestrians and Bicycles",
                        np.where(df[desc_col].str.contains("Mitigation of Water Pollution due to Highway Runoff"), "Mitigation of Water Pollution due to Highway Runoff",
                        np.where(df[desc_col].str.contains("Traffic Management/Engineering - HOV"), "Traffic Management Project",
                        np.where(df[desc_col].str.contains("Planning "), "Project Planning",
                        np.where(df[desc_col].str.contains("4R - Restoration & Rehabilitation"), "Road Restoration & Rehabilitation",
                        np.where(df[desc_col].str.contains("4R - Maintenance  Resurfacing"), "Maintenance Resurfacing",
                        np.where(df[desc_col].str.contains("4R - Added Capacity"), "Added Roadway Capacity",
                        np.where(df[desc_col].str.contains("4R - No Added Capacity"), "Road Construction",
                        np.where(df[desc_col].str.contains("Safety"), "Safety Improvements",
                        np.where(df[desc_col].str.contains("New  Construction Roadway"), "New Construction Roadway",
                        np.where(df[desc_col].str.contains("Preliminary Engineering"), "Preliminary Engineering Projects",
                        np.where(df[desc_col].str.contains("Construction Engineering"), "Construction Engineering Projects",
                        np.where(df[desc_col].str.contains("Right of Way"), "Right of Way Project",
                        np.where(df[desc_col].str.contains("Administrative Expenses"), "Administrative Expenses",
                                    "Project")))))))))))))))))))
    
    return df

## function for reading in the locodes
def read_in_locodes():
    locodes = to_snakecase(pd.read_excel(f"gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/locodes_updated7122021.xlsx"))
    locodes['agency_name'] = locodes['agency_name'].str.upper()
    locode_names = sorted(list(locodes['agency_name'].unique()), reverse=True)    
    
    ## append list so that "Crosswalk" does not select "Ross"
    locode_names.remove('ROSS')
    locode_names.append("STATE PARKS")
    locode_names.append("SACOG - Sacramento Area Council of Governments")
    locode_names.append("SACOG")
    locode_names.append("MTC")
    locode_names.append("MTC - Metropolitan Transportation Commission")
    locode_names.append("Statewide")
    
    return locode_names

## function to find any agency name/locations wihin the description column or title column using locodes
def find_alternative_name(df, desc_col, new_col_name):
    
    #read in locode names to get list to find in description column
    locode_names = read_in_locodes()
    
    ### https://stackoverflow.com/questions/68869434/create-an-pandas-column-if-a-string-from-a-list-matches-from-another-column
    df[new_col_name] = df[desc_col].map(lambda s: next((name for name in locode_names if name in s), ""))
    df[new_col_name] = df[new_col_name].str.title()
    
    return df


#function for getting title column

## changing the title function to include the new description col
def add_new_title(df, first_col_method, second_col_type, third_col_name, alt_col_name, alt_in_proj_desc):
    """
    Function to add new title. 
    Expected output example: "New Bike Lane in Eureka"
    """    

    
    def return_name(df):
        
        if (df[third_col_name] == "California") & (df[alt_col_name] == "Statewide") & (df[alt_in_proj_desc] != ""):
            return (df[first_col_method] + " " + df[second_col_type] +" in " + df[alt_in_proj_desc])
        
        elif (df[third_col_name] == "California") & (df[alt_col_name] == "Statewide") & (df[alt_in_proj_desc] == ""):
            return (df[first_col_method] + " " + df[second_col_type] +" " + df[alt_col_name])
                    
        elif (df[third_col_name]== "Unknown"):
            return (df[first_col_method] + " " + df[second_col_type] + " in " + df[alt_in_proj_desc])
        
        elif (df[third_col_name] == "California"):
            return (df[first_col_method] + " " + df[second_col_type] + " in " + df[alt_col_name])
        
        elif (df[third_col_name] == "Metropolitan Transportation Commission"):
            return (df[first_col_method] + " " + df[second_col_type] + " in the " + df[third_col_name] + " Region")
        
        elif (df[third_col_name] == "Los Angeles County Metropolitan Transportation Authority"):
            return (df[first_col_method] + " " + df[second_col_type] + " in the " + df[third_col_name] + " Region")
        
        elif (df[third_col_name] == "Caltrans") & (df[alt_in_proj_desc] != ""):
            return (df[first_col_method] + " " + df[second_col_type] + " in " + df[alt_in_proj_desc])
        
        elif (df[third_col_name] == "Caltrans") & (df[alt_in_proj_desc] == ""):
            return (df[third_col_name] + " " + df[first_col_method] + " " + df[second_col_type])
        
        elif (df[third_col_name] != "California"):
            return (df[first_col_method] + " " + df[second_col_type] + " in " + df[third_col_name])
        

        return df

    df['project_name_new'] = df.apply(return_name, axis = 1)
    
    return df


def get_new_desc_title(df):
    '''
    function takes the unique project ids and uses the title functions above to create a
    unique title for each project (each project can have multiple phases).
    this function then maps back to the original df
    
    note: we can only use this function once in the notebook. second time running will throw an error. 
    '''
    df_copy = df.copy()
    #add descriptions
    proj_unique_cat = add_description(df_copy, 'project_title')
    
    #remove project method column values so that the title function wont double count
    proj_unique_cat.loc[proj_unique_cat['project_type'] == 'Project', 'project_method'] = ""
    proj_unique_cat['project_type'] = proj_unique_cat['project_type'].replace('Project', np.NaN)
    
    #update for the projects not in the first round of descriptions
    proj_unique_cat_title =  add_description_4_no_match(proj_unique_cat, 'improvement_type_description')
    ## proj_unique_cat_title = update_no_matched(proj_unique_cat, "project_type", 'improvement_type_description', 'program_code_description')
    
    #fill nan values in 'Project_type' with values from 'project_type2' from add_description_4_no_match function
    proj_unique_cat_title['project_type'] = proj_unique_cat_title['project_type'].fillna(proj_unique_cat_title['project_type2'])
    
    
    # first get new columns to get name to replace the Unknowns
    #replace values for implementing agency name that are "unknown" with names in the project description
    #get list of agency names from locode document
    ### https://stackoverflow.com/questions/68869434/create-an-pandas-column-if-a-string-from-a-list-matches-from-another-column
    proj_unique_cat = find_alternative_name(proj_unique_cat, "project_title", "alt_geo_name_projdesc")
    
    #add title - second round to account for statewide projects
    proj_unique_cat_title = add_new_title(proj_unique_cat, "project_method", "project_type", "implementing_agency", "county_name_title", "alt_geo_name_projdesc")
    
    # rename new title one
    proj_unique_cat_title = proj_unique_cat_title.rename(columns={'project_name_new':'new_project_title'})
    # proj_unique_cat_title.drop(columns =['project_method', 'project_type', 'project_type2'], axis=1, inplace=True)
    
    #map the title back to df
    proj_title_mapping = (dict(proj_unique_cat_title[['project_number', 'new_project_title']].values))
    
    df['new_project_title'] = df.project_number.map(proj_title_mapping)
    
    df.rename(columns={'project_title': 'old_project_title_desc'}, inplace=True)

    return df

## function to add new description column that has the federal program amount and the dollar amount. 
## to run after aggregating the data
def add_new_description_col(df):
    df["obligations_amount_string"] = df["obligations_amount"].astype(str)
    
    # df["new_description_col"] = "This project is part of the " + df["program_code_description"] + " Program, and recieved $" + df["obligations_amount_string"] + ". This project will " + df["new_project_title"] + "."
    
    df["new_description_col"] = df["new_project_title"] + ", part of the " + df["program_code_description"] + ". (Federal Project ID: " + df["project_number"] + ")."
    
    df.drop(columns =['obligations_amount_string', 'county_name_title'], axis=1, inplace=True)
    df['implementing_agency_locode'] = df['implementing_agency_locode'].str.replace('.0', '')
    
    return df

def get_clean_data(df, full_or_agg = ''):
    
    '''
    Function putting together all the functions. 
    Returns data with new title, known agency name, and agency information.
    
    full_or_agg = 'agg' 
        To return an aggregated df (one row for each project)
        
    full_or_agg = 'full' 
        To return a full df with all rows
        
    default- return agg df
    '''
    
    if full_or_agg == 'agg':

        ## function that adds known agency name to df 
        # df = identify_agency(df, 'summary_recipient_defined_text_field_1_value')
        
        df = format_congressional_district(df, "congressional_district")
        
        df = change_district_format(df, "district")
                
        df= add_county_abbrev(df, 'county_name')
        
        aggdf = condense_df(df)
    
        ## get new title (str parser) 
        aggdf = get_new_desc_title(aggdf)
        
        aggdf = add_new_description_col(aggdf)
        
        # _data_utils.change_col_to_integer(df, "congressional_district")
    
        return aggdf
    
    elif full_or_agg == 'full':
    
        ## function that adds known agency name to df 
        # df = identify_agency(df, 'summary_recipient_defined_text_field_1_value')
        
        df = format_congressional_district(df, "congressional_district")
        
        df = change_district_format(df, "district")
        
        df= add_county_abbrev(df, 'county_name')
        
        aggdf = condense_df(df)
        
        aggdf = get_new_desc_title(aggdf)
        
        #map title back to full df
        proj_title_mapping = (dict(aggdf[['project_number', 'new_project_title']].values))
    
        df['new_project_title'] = df.project_number.map(proj_title_mapping)
        
        ##asserting that the there is one row for each project id in the new 
        assert len(agg) == proj.project_number.nunique()

        return df
    
    


    
# '''
# another approach (not as effective for creating new titles)
# '''
# ## code help: https://stackoverflow.com/questions/70995812/extract-keyword-from-sentences-in-a-pandas-text-column-using-nltk-and-or-regex
# def key_word_intersection(df, text_col):
#     summaries = []
#     for x in tokenize(df[text_col].to_numpy()):
#         keywords = np.concatenate([
#                                 np.intersect1d(x, ['BRIDGE REPLACEMENT', 'BRIDGE', 'INSTALL', 'CONSTRUCT', 'REPLACE',
#                                                    'SIGNAL', 'SIGNALS', 'TRAFFIC', 'IMPROVEMENT', 'PEDESTRIAN', 
#                                                    'LANES', 'NEW', 'REHABILITATION','UPGRADE', 'CLASS',
#                                                    'BIKE', 'WIDEN', 'LANDSCAPING', 'SAFETY', 'RAISED', 
#                                                    'SEISMIC', 'SIGNAGE', 'RETROFIT', 'ADD', 'PLANNING', 'PAVE',
#                                                    'PREVENTIVE','MAINTENANCE', 'REHAB', 'RESURFACE', 'REPAIR', 'ROUNDABOUT'
#                                                   'COMPLETE STREET', 'VIDEO DETECTION EQUIPMENT', 'SYNCHRONIZE CORRIDOR', 'ROADWAY REALIGNMENTS']),
#                                 np.intersect1d(x, [
#                                     # 'BRIDGE', 'ROAD', 'RD', 'AVENUE', 'AVE', 'STREET' , 'ST',
#                                                    # 'FRACTURED', 'LANE', 'DRIVE', 'BOULEVARD', 'BLVD',
#                                                    'INTERSECTION', 'INTERSECTIONS', 'SIDEWALK', 
#                                     # 'WAY', 'DR', 'CURB', 'ROADWAY',
#                                                    # 'TRAIL', 'PATH', 'CREEK', 'RIVER', 
#                                     # 'CORRIDOR', 'CROSSING','PARKWAY','RAMPS', 'GUARDRAIL'
#                                 ]), 
#                                 np.intersect1d(x, ['CITY', 'COUNTY', 'STATE', 'UNINCORPORATED'])])
    
#         summaries.append(np.array(x)[[i for i, keyword in enumerate(x) if keyword in keywords]])
#     return summaries 
