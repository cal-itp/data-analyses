'''
Cleaning data for Consolidated Application
There are 4 different cleaned dataframes. 
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
Caltrans_shape = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"

from calitp.storage import get_fs
fs = get_fs()

'''
Functions for Initial Clean Up 
'''
#Function for comparing total funding amount versus total estimated expense
def funding_vs_expenses(df):
    if df["total_state_federal_local_funding"] == df["total_expenses"]:
        return "Fully funded"
    elif df["total_state_federal_local_funding"] > df["total_expenses"]:
        return "Funding exceeds total expenses"
    else:
        return "Not fully funded"

'''
Import & Load
'''
def load_con_app():
    #We keep all the columns 
    con_app_file =  "Copy of Application_Review_Report_5_2_2022.xls"
    con_app =  to_snakecase(
    pd.read_excel(f"{GCS_FILE_PATH}{con_app_file}"))
    return con_app

'''
Initial Clean Up 
An unpivoted & unaggregated, but cleaned up version of the df 
'''
def initial_cleaning(df):
    ### ORG NAMES ###
    #Replace Ventura County since it read in strangely
    df["organization_name"] = df["organization_name"].replace(
    {"Ventura County Transportation Commission\xa0": "Ventura County Transportation Commission"}).str.replace(
    "\s+\(.*$", "", regex=True)
    
    # Remove any acronyms
    #df["organization_name"] = df["organization_name"].str.replace("\s+\(.*$", "", regex=True)
   
    ### PROJECT CATEGORIES ### 
    #Spell out project categories 
    df["project_category"] = df["project_category"].replace(
    {"OP": "Operating", "CA": "Capital", "PL": "Planning", "CM": "Capital Maintenance"})
    
    #Project categories are pretty vague, but project description has 200+ diff inputs
    #Search through descriptions for the keywords below and input keyword into the new column "short description"
    df["project_description"] = df["project_description"].str.lower()
    df["short_description"] = df["project_description"].str.extract(
    "(operating|bus|construction|buses|planning|van|vessel|fare|ridership|vehicle|station|service|equipment|maintenance|surveillance|renovate|free|equip|operational)",
    expand=False)
    #Replace the keywords with the main categories
    #Capture any entries that don't fall into a particular category as "other" 
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
        "surveillance": "purchasing other tech"})
    
    df["short_description"] = (
    df["short_description"].fillna("other category").str.title())
    ### MONETARY COLS ### 
    #Local totals: split on ":" and extract only the last item
    #To grab the total of local funding a proejct will have/has
    df["local_total"] = df["local_total"].str.split(": ").str[-1]
    
    #Remove $ and , turn column from str into float
    df["local_total"] = (
    df["local_total"]
    .str.replace(",", "", regex=True)
    .str.replace("$", "", regex= True)
    .fillna(0)
    .astype("float")) 
        
    #List of original monetary columns: excludes ones I've created
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
    # and total for state and local funds only
    df = df.assign(
    total_state_federal_local_funding = (df["state_total"]
    + df["local_total"]
    + df["federal_total"]
    + df["other_fed_funds_total"]),    
    total_state_fed_only = 
    (df["state_total"] + df["federal_total"])) 
    
    #Apply function to determine if a project is fully funded or not
    df['fully_funded'] = df.apply(funding_vs_expenses, axis=1)
    
     
    ### DISTRICTS ### 
    #Find any rows with missing values in the district column - will change from year to year 
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
Melted Dataframe
The original df has a row for each unique project & columns for each fund 
regardless of whether or not a project is requesting that fund.
Thus, we should clean up df from wide to long. 
'''
def melt_df(df): 
    # Keep only subset of what I want to melt & the identifier column 
    # Which is the project_upin: a unique identifier for each product
    
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
def group_df(melted_df, initial_clean_df):
    # Exclude totals: not a fund 
    grouped1 = melted_df.loc[
    ~melted_df["program_name"].isin(
        [
            "Local Funds",
            "Federal Total",
            "State Total",]
    )]
    
    #Grab all the different program names by project upin and put it in a new column
    grouped1["all_programs"] = grouped1.groupby("project_upin")["program_name"].transform(
    lambda x: ",".join(x))
    
    #Keep only cols of interest & drop duplicates
    grouped1 = grouped1[["project_upin", "all_programs"]].drop_duplicates()
    
    # Merge with original dataframe because above we only have project_upin and all the funds left
    grouped2 = pd.merge(grouped1, initial_clean_df, on="project_upin", how="left")
    
    # Keep only relevant cols
    grouped2 = grouped2[
    ["project_upin", "organization_name", "project_description", "all_programs", "year"]]
    
    # Count # of funds under "all programs" column to get a metric of how many funds 
    #Orgs want for a particular project
    # https://stackoverflow.com/questions/51502263/pandas-dataframe-object-has-no-attribute-str
    grouped2["count_of_funding_programs_applied"] = (
    grouped2["all_programs"]
    .str.split(",+")
    .str.len()
    .groupby(grouped2.project_upin)
    .transform("sum"))
    
    return grouped2 

'''
Geodataframe for the map
'''
def gdf_conapp(df):
    #Load geojson with the shapes of the Caltrans districts
    geojson = (gpd.read_file(f'{Caltrans_shape}')
               .to_crs(epsg=4326))
    
    #Keep only the columns we want 
    geojson = geojson[["DISTRICT", "Shape_Length", "Shape_Area", "geometry"]]
    
    #Take the cleaned, unaggregated dataframe and get summarize statistics
    summarized = df.groupby("district").agg(
    {"project_upin": "count", "total_state_fed_only": "sum"}).reset_index()
    
    #New column that rounds total_state_fed to millions
    summarized["funding_millions"] = (
    "$"
    + (summarized["total_state_fed_only"].astype(float) / 1000000)
    .round()
    .astype(str)
    + "M")
    
    #For the map, it looks nicer when the legend is pinned to percentiles instead of 
    #actual dollar amounts.
    p75 = summarized.total_state_fed_only.quantile(0.75).astype(float)
    p25 =summarized.total_state_fed_only.quantile(0.25).astype(float)
    p50 = summarized.total_state_fed_only.quantile(0.50).astype(float)
    
    #Function for mapping percentiles 
    def funding_range(row):
        if ((row.total_state_fed_only > 0) and (row.total_state_fed_only < p25)):
            return "25"
        elif ((row.total_state_fed_only > p25) and (row.total_state_fed_only < p75)):
            return "50"
        elif ((row.total_state_fed_only > p50) and (row.total_state_fed_only > p75 )):
               return "75"
        else:
            return "No Info"
        
    #Apply the aforementioned function into a new column
    summarized["funding_percentile"] = summarized.apply(lambda x: funding_range(x), axis=1)
    
    #Merge geojson with the summarized df
    gdf = geojson.merge(
    summarized, how="inner", left_on="DISTRICT", right_on="district") 
    
    #Export 
    shared_utils.utils.geoparquet_gcs_export(gdf, f'{GCS_FILE_PATH}',
    "script_con_app_gdf")
    
    return summarized

'''
Function to build all 4 dataframes together
'''
def con_app_complete_clean():
    #Load in original sheet
    raw_con_app = load_con_app() 
    
    #Do the initial cleaning
    cleaned_con_app = initial_cleaning(raw_con_app) 
    
    #First aggregation: melting the dataframe
    melted_df = melt_df(cleaned_con_app)
    
    #Second aggregation: putting all funding programs onto a single line  
    grouped_df = group_df(melted_df, cleaned_con_app)
    
    #Third aggregation: summarize and turn it into a gdf that will be saved
    #as a geoparquet to GCS but will return a regular dataframe for previewing
    gdf = gdf_conapp(cleaned_con_app)
    
    #Write the first 3 dfs into an Excel workbook  and save to GCS in case
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Script_Testing.xlsx") as writer:
        melted_df.to_excel(writer, sheet_name="pivoted_data", index=False)
        cleaned_con_app.to_excel(writer, sheet_name="cleaned_unpivoted_data", index=False)
        grouped_df.to_excel(writer, sheet_name="combos_of_funding_programs", index=False)
    
    #Return everything
    return cleaned_con_app, melted_df, grouped_df,  gdf
