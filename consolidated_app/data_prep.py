"""
Data prep
"""
import re as re

import numpy as np
import pandas as pd

pd.options.display.max_columns = 50
pd.options.display.max_rows = 250
pd.set_option("display.max_colwidth", None)
pd.options.display.float_format = "{:.2f}".format

from itertools import chain
from collections import Counter
from itertools import combinations

from calitp import *
from siuba import *

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/consolidated_applications/"

def clean_consolidated_app(): 
    # LOAD IN DATA #
    con_app = "Copy of Application_Review_Report_5_2_2022.xls"
    data = to_snakecase(
    pd.read_excel(f"{GCS_FILE_PATH}{con_app}")
    )
    
    ### CLEAN UP ORGANIZATION NAME ###
    # Strip organizations of any acronyms
    data["organization_name"] = data["organization_name"].str.replace(
    "\s+\(.*$", "", regex=True)
    
    # Replace Ventura, as it read in with extra characters
    data["organization_name"] = data["organization_name"].replace(
    {"Ventura County Transportation Commission\xa0": 
     "Ventura County Transportation Commission"})
    
    ### CLEAN UP PROJECT COLUMNS ### 
    # Replace project category with full name for readability. 
    data["project_category"] = data["project_category"].replace(
    {
        "OP": "Operating",
        "CA": "Capital",
    })
    
    # There are 200+ unique project descriptions, narrow them down
    # First search through project description for keywords
    data["short_description"] = data["project_description"].str.extract("(operating|bus|construction|buses|planning|van|vessel|fares|ridership|vehicle|station|service|equipment|maintenance|surveillance|renovate|free|equip|operational)",
    expand=False,)
    # Second, group down the keywords into even broader categories. 
    data["short_description"] = data["short_description"].replace(
    {
        "operating": "operating assistance",
        "operational": "operating assistance",
        "free": "free fare program",
        "ridership": "ridership expansion",
        "fare": "free fare programs",
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
        "surveillance": "purchasing other tech",
    })
    # Tag any projects that weren't tagged under "other category"
    data["short_description"] = data["short_description"].fillna("other category")
    
    # Function for proper capitalization 
    data["short_description"]= data["short_description"].str.title()
    
    ### MONETARY COLUMNS ### 
    # The column "local total" reads in weirdly and have 
    # multi lines in one cell. Keep only the integers after "Local Total": 
    data["local_total"] = data["local_total"].str.split(": ").str[-1]
    
    # Remove commas and dollar signs, change local total to the right data type.
    data["local_total"] = (
    data["local_total"]
    .str.replace(",", "", regex=True)
    .str.replace("$", "", regex=True)
    .fillna(0)
    .astype("float"))
    
    # Change other monetary columns to be right data type 
    data[monetary_cols] = (
    data[monetary_cols]
    .fillna(value=0)
    .apply(pd.to_numeric, errors="coerce")
    .astype("float"))
    
    # Create a new column that adds up the total state, local, and fed funding
    # an organization has received.
    data["total_state_federal_local_funding"] = (
    data["state_total"]
    + data["local_total"]
    + data["federal_total"]
    + data['other_state_funds']
    + data['other_fed_funds_total'])
    
    # Function to flag whether a project is fully funded or not, by comparing 
    # the columns "total expenses" against "total_state_federal_local_fudning"
    def funding_vs_expenses(df):
    if df["total_state_federal_local_funding"] == df["total_expenses"]:
        return "Fully funded"
    elif df["total_state_federal_local_funding"] > df["total_expenses"]:
        return "Funding exceeds total expenses"
    else:
        return "Not fully funded"
    
    data["fully_funded"] = data.apply(funding_vs_expenses, axis=1)
    
    ### PIVOT DATAFRAME ###
    # I want to pivot the dataframe so it is in a long format instead of wide
    # so I can more easily see the combination of funding programs an org applied for.
    
    # Create new dataframe with the columns I want to melt 
    monetary_subset = data[
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
        "local_total"]]
    
    # Melt the columns and rename it
    monetary_subset = pd.melt(
    monetary_subset,
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
    ],
    var_name="program_name",
    value_name="funding_received",)
    
    # Create a subset of the original dataframe because when I merge the 
    # Melted dataframe with the original one, I only want certain columns
    data2 = data[
    [
        "total_expenses",
        "organization_name",
        "district",
        "year",
        "application_status",
        "project_upin",
        "project_category",
        "project_line_item__ali_",
        "project_description",
        "is_stimulus",
        "total_state_federal_local_funding",
        "fully_funded",
        "short_description"]]
    
    # Merge original dataframe with melted dataframe 
    merge1 = pd.merge(monetary_subset, data2, on="project_upin", how="left")
    
    # Neaten program names
    merge1["program_name"] = merge1["program_name"].replace(
    {
        '_5311_funds':'5311 (Fed)',
        'lctop__state__funds': 'LCTOP (State)',
        'transit_development_act__state__funds':'Transit Development Act (State)',
        'other_state_funds':'Other State Funds',
        '_5339_funds': '5339 (Fed)',
        '_5311_f__funds': '5311(f) (Fed)',
        'sb1__state_of_good_repair__state__funds': 'SB1. State of Good Repair (State)',
        'other_fed_funds_total': 'Other Federal Funds',
        '_5311_cmaq_funds': '5311 CMAQ (Fed)',
        'local_total': 'Local Funds'})
    
    # Filter any zeroes in the funding received column - any rows with $0 would be 
    # irrelevant to our analysis of what grant an organization applied for and what funding it received
    melt_df = merge1[merge1["funding_received"] > 0]
    
    ### FIND GROUPINGS ###
    # Part of the goal is to find the combination of the grant programs organizations 
    # applied to using the Consolidated Application.
    
    # Filter out local funds - included it in the melt because its one of the funding streams
    group = melt_df.loc[melt_df["program_name"] != "Local Funds"]
    
    # Grab all the different program names by project upin and put it in a new column
    group["all_programs"] = group.groupby("project_upin")["program_name"].transform(
    lambda x: ",".join(x))
    
    # Take new data frame and merge it with the original, un-melted dataframe 
    # So we can get organization name & other columns again.
    grouped_df = pd.merge(group, data, on="project_upin", how="left")
    
    # Get the count of how many grants each organization applied for by UPIN number. 
    grouped_df["count_of_funding_programs_applied"] = (
        grouped_df["all_programs"]
        .str.split(",+")
        .str.len()
        .groupby(grouped_df.project_upin)
        .transform("sum"))
    
    ### EXPORT ### 
    # Export all 3 dataframes
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Con_App_Cleaned.xlsx") as writer:
        melt_df.to_excel(writer, sheet_name="pivoted_data", index=False)
        data.to_excel(writer, sheet_name="cleaned_unpivoted_data", index=False)
        grouped_df.to_excel(writer, sheet_name="combos_of_funding_programs", index=False)
    return data 


