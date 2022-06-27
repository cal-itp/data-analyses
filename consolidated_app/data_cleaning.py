'''
Cleaning data for Consolidated Application
There are three different cleaned dataframes. 
'''
import os
import re as re

import geopandas as gpd
import numpy as np
import pandas as pd

from collections import Counter
from itertools import chain, combinations

import shared_utils
from calitp import *
from shared_utils import utils
from siuba import *

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/consolidated_applications/"
FILE =  "Copy of Application_Review_Report_5_2_2022.xls"

from calitp.storage import get_fs
fs = get_fs()

'''
Cleaning up Project Descriptions & Project Names
'''
def clean_project_desc():
    df = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}{FILE}'))
    
    #Organization names 
    #Replace Ventura County since it read in strangely
    df["organization_name"] = df["organization_name"].replace(
    {"Ventura County Transportation Commission\xa0": "Ventura County Transportation Commission"})
    
    # Remove any acronyms
    df["organization_name"] = df["organization_name"].str.replace(
    "\s+\(.*$", "", regex=True)
   
    #There are hundreds of project descriptions: organize to fewer categories.
    #Change all strings to lowercase so we can search through properly
    df["project_description"] = df["project_description"].str.lower()
    
    #Search through descriptions for the keywords below and input keyword into the new column "short description"
    df["short_description"] = df["project_description"].str.extract(
    "(operating|bus|construction|buses|planning|van|vessel|fare|ridership|vehicle|station|service|equipment|maintenance|surveillance|renovate|free|equip|operational)",
    expand=False,)
    
    #Replace the keywords with the main categories
    #Capture any entries that don't fall into a particular category.
    #Change this column to title case for cleaner look
    df["short_description"] = df["short_description"].replace(
    {
        "operating": "operating assistance",
        "operational": "operating assistance",
        "free": "free fare program",
        "ridership": "ridership expansion",
        "fare": "purchasing other tech",
        "service": "service expansion",
        "buses": "purchasing vehicles",
        "bus": "purchasing vehicles",
        "van": "purchasing vehicles",
        "vessel": "purchasing vehicles",
        "vehicles": "purchasing vehicles",
        "vehicle": "purchasing vehicles",
        "planning": "transit planning",
        "station": "construction",
        "construction": "construction",
        "maintenance": "maintenance/renovation",
        "renovate": "maintenance/renovation",
        "equipment": "purchasing other tech",
        "equip": "purchasing other tech",
        "surveillance": "purchasing other tech"}).fillna("other category").str.title())
    
    return df

'''
Cleaning up Monetary columns
'''
#Function for comparing total funding amount versus total estimated expense
def funding_vs_expenses(df):
    if df["total_state_federal_local_funding"] == df["total_expenses"]:
        return "Fully funded"
    elif df["total_state_federal_local_funding"] > df["total_expenses"]:
        return "Funding exceeds total expenses"
    else:
        return "Not fully funded"
    
def clean_monetary(df):
    #Local totals: split on ":" and extract only the last item
    #To grab the total of local funding a proejct will have/has
    df["local_total"] = df["local_total"].str.split(": ").str[-1]
    
    #Remove $ and , turn column from str into float
    df["local_total"] = (
    df["local_total"]
    .str.replace("(,|$)", "", regex=True)
    .fillna(0)
    .astype("float")) 
    
    #Grab list of monetary cols
    monetary_cols = [
    "total_expenses",
    "_5311_funds",
    "_5311_f__funds",
    "_5311_cmaq_funds",
    "_5339_funds",
    "federal_total",
    "other_fed_funds_total",
    "lctop__state__funds",
    "sb1__state_of_good_repair__state__funds",
    "transit_development_act__state__funds",
    "other_state_funds",
    "state_total"]
    
    #Clean them all up
    df[monetary_cols] = (
    df[monetary_cols]
    .fillna(value=0)
    .apply(pd.to_numeric, errors="coerce")
    .astype("float"))
    
    #Create three new cols: total for local, state, and fed
    #total for state and local funds only
    #whether a project is fully funded or not 
    df = df.assign(
    total_state_federal_local_funding = (df["state_total"]
    + df["local_total"]
    + df["federal_total"]
    + df["other_fed_funds_total"]),    
    total_state_fed_only = 
    (df["state_total"] + df["federal_total"]), 
    fully_funded = df.apply(funding_vs_expenses, axis=1)
    ) 
    return df 

'''
Cleaning up Districts
'''
def clean_districts(df):
    #Find any rows with missing values in the district column: 
    #no_districts = data[data["district"].isnull()]
    #no_districts_list = no_districts["project_upin"].tolist()
    
    #Replace the districts by organization names
    df.loc[(df["organization_name"] == "City of Banning"), "district"] = 8
    df.loc[(df["organization_name"] == "City of Clovis"), "district"] = 6
    df.loc[(df["organization_name"] == "City of Los Angeles DOT"), "district"] = 7
    df.loc[(df["organization_name"] == "Peninsula Corridor Joint Powers Board"), "district"] = 4
    df.loc[(df["organization_name"] == "San Joaquin Regional Rail Commission"), "district"] = 10
    df.loc[(df["organization_name"] == "Western Contra Costa Transit Authority"), "district"] = 4
    
    # Create new column with fully spelled out names
    df["full_district_name"] = df["district"].replace(
    {
        7: "District 7: Los Angeles",
        4: "District 4: Bay Area / Oakland",
        2: "District 2: Redding",
        9: "District 9: Bishop",
        10: "District 10: Stockton",
        11: "District 11: San Diego",
        3: "District 3: Marysville / Sacramento",
        12: "District 12: Orange County",
        8: "District 8: San Bernardino / Riverside",
        5: "District 5: San Luis Obispo / Santa Barbara",
        6: "District 6: Fresno / Bakersfield",
        1: "District 1: Eureka",
    })
    
    return df 

'''
Fully Cleaned Dataset
'''
def fully_clean():
    df1 = clean_project_desc()
    df2 = clean_monetary(df1)
    cleaned_unpivoted_df = clean_districts(df2)
    return cleaned_unpivoted_df 

'''
Melted Dataframe
The original df has a row for each unique project & columns for each fund 
regardless of whether or not a project is requesting that fund.
Clean up df from wide to long
'''
def melt_df(): 
    # Keep only subset of what I want to melt & the identifier column 
    # Which is the project_upin: a unique identifier for each product
    
    #Load in clean dataset 
    df = fully_clean() 
    
    melt_subset1 = df[
    [
        "project_upin",
        "_5311_funds",
        "_5311_f__funds",
        "_5311_cmaq_funds",
        "_5339_funds",
        "lctop__state__funds",
        "sb1__state_of_good_repair__state__funds",
        "transit_development_act__state__funds",
        "other_state_funds",
        "other_fed_funds_total",
        "local_total",
        "federal_total",
        "state_total",
    ]]
    
    #Melt the df: put funds (value_vars) beneath value_name and the
    #associated funding amounts under the column "funding received."
    melt_subset2 = pd.melt(
    melt_subset1, #subsetted df 
    id_vars=["project_upin"],
    value_vars=[
        "_5311_funds",
        "_5311_f__funds",
        "_5311_cmaq_funds",
        "_5339_funds",
        "lctop__state__funds",
        "sb1__state_of_good_repair__state__funds",
        "transit_development_act__state__funds",
        "other_state_funds",
        "other_fed_funds_total",
        "local_total",
        "federal_total",
        "state_total",
    ],
    var_name="program_name",
    value_name="funding_received",)
    
    # Create a subset of the original df 
    # To merge it onto our melted df so we can info such as project description
    # Fully funded or not, etc 
    df2 = df[
    [
        "total_expenses",
        "organization_name",
        "district",
        "full_district_name",
        "year",
        "application_status",
        "project_upin",
        "project_category",
        "project_line_item__ali_",
        "project_description",
        "is_stimulus",
        "total_state_federal_local_funding",
        "fully_funded",
        "short_description",
    ]]
    
    # Left merge with melted dataframe, which will has MANY more lines 
    m1 = pd.merge(melt_subset2, df2, on="project_upin", how="left")
    
    # Rename funds for clarity 
    m1["program_name"] = m1["program_name"].replace(
    {
        "_5311_funds": "5311 (Fed)",
        "lctop__state__funds": "LCTOP (State)",
        "transit_development_act__state__funds": "Transit Development Act (State)",
        "other_state_funds": "Other State Funds",
        "_5339_funds": "5339 (Fed)",
        "_5311_f__funds": "5311(f) (Fed)",
        "sb1__state_of_good_repair__state__funds": "SB1. State of Good Repair (State)",
        "other_fed_funds_total": "Other Federal Funds",
        "_5311_cmaq_funds": "5311 CMAQ (Fed)",
        "local_total": "Local Funds",
        "federal_total": "Federal Total",
        "state_total": "State Total",
    }) 
    
    # Filter out excess rows with $0 in the col "funding_received"
    # To shorten dataframe 
    m1 = m1[m1["funding_received"] > 0]
    return m1 

'''
Grouped Dataframe
Building off of the melted dataframe, take all the funds 
a project is asking for and put the funds on one line.
This way we can see analyze the combos of the various funds 
orgs are applying for 
'''
def grouped_df():
    melt = melt_df() 
    original_df = fully_clean() 
    # Exclude totals: not a fund 
    grouped1 = melt.loc[
    ~melt["program_name"].isin(
        [
            "Local Funds",
            "Federal Total",
            "State Total",]
    )]
    
    #Grab all the different program names by project upin and put it in a new column
    #Drop duplicates
    grouped1["all_programs"] = grouped1.groupby("project_upin")["program_name"].transform(
    lambda x: ",".join(x)).drop_duplicates()
    
    # Merge with original dataframe because above we only have project_upin and all the funds left
    grouped2 = pd.merge(group, original_df, on="project_upin", how="left")
    
    # Keep only relevant cols
    grouped2 = grouped2[
    ["project_upin", "organization_name", "project_description", "all_programs", "year"]]
    
    # Count # of funds under "all programs" column 
    # https://stackoverflow.com/questions/51502263/pandas-dataframe-object-has-no-attribute-str
    grouped2["count_of_funding_programs_applied"] = (
    grouped2["all_programs"]
    .str.split(",+")
    .str.len()
    .groupby(grouped_df.project_upin)
    .transform("sum"))
    
    return grouped2 







    
    
