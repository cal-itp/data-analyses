"""
Tableau 
"""
import numpy as np
import pandas as pd
from siuba import *
from calitp import *
from plotnine import *
import intake
import shared_utils

import altair as alt
import altair_saver
from shared_utils import geography_utils
from shared_utils import altair_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide

#GCS File Path:
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/tircp/"
FILE_NAME = "TIRCP_July_8_2022.xlsx"

#Crosswalk
import crosswalks 

#Import cleaned up data
import A1_data_prep

'''
Functions
'''
#Categorizing expended percentage into bins
def expended_percent(row):
    if (row.Expended_Percent > 0) and (row.Expended_Percent < 0.26):
        return "1-25"
    elif (row.Expended_Percent > 0.25) and (row.Expended_Percent < 0.51):
        return "26-50"
    elif (row.Expended_Percent > 0.50) and (row.Expended_Percent < 0.76):
        return "51-75"
    elif (row.Expended_Percent > 0.75) and (row.Expended_Percent < 0.95):
        return "76-99"
    elif row.Expended_Percent == 0.0:
         return "0"
    else:
        return "100"
    
# Categorize years and expended_percent_group into bins
def progress(df):
    ### 2015 ###
    if (df["project_award_year"] == 2015) and (df["Expended_Percent_Group"] == "1-25") | (
            df["Expended_Percent_Group"] == "26-50"
        ):
           return "Behind"
    elif (df["project_award_year"] == 2015) and (
            df["Expended_Percent_Group"] == "76-99"
        ) | (df["Expended_Percent_Group"] == "51-75"):
         return "On Track"

     ### 2016 ###
    elif (df["project_award_year"] == 2016) and (df["Expended_Percent_Group"] == "1-25") | (
            df["Expended_Percent_Group"] == "26-50"
        ):
        return "Behind"
    elif (df["project_award_year"] == 2016) and (
            df["Expended_Percent_Group"] == "51-75"
        ) | (df["Expended_Percent_Group"] == "76-99"):
         return "On Track"

     ### 2018 ###
    elif (df["project_award_year"] == 2018) and (df["Expended_Percent_Group"] == "1-25"):
        return "Behind"
    elif (df["project_award_year"] == 2018) and (
            df["Expended_Percent_Group"] == "26-50"
        ) | (df["Expended_Percent_Group"] == "51-75"):
        return "On Track"
    elif (df["project_award_year"] == 2018) and (df["Expended_Percent_Group"] == "76-99"):
         return "Ahead"

     ### 2020 ###
    elif (df["project_award_year"] == 2020) and (df["Expended_Percent_Group"] == "1-25"):
           return "Behind"
    elif (df["project_award_year"] == 2020) and (df["Expended_Percent_Group"] == "26-50"):
           return "On Track"
    elif (df["project_award_year"] == 2020) and (
            df["Expended_Percent_Group"] == "51-75"
        ) | (df["Expended_Percent_Group"] == "76-99"):
            return "Ahead"

    ### 0 Expenditures ###
    elif df["Expended_Percent_Group"] == "0":
        return "No expenditures recorded"

    ### Else ###
    else:
        return "100% of allocated funds spent"
'''
Columns
'''
columns_to_keep = ['project_award_year','project_grant_recipient',
       'project_project_title', 'project_ppno', 'project_district',
       'project_technical_assistance_calitp__y_n_',
       'project_technical_assistance_fleet__y_n_',
       'project_technical_assistance_network_integration__y_n_',
       'project_technical_assistance_priority_population__y_n_',
       'project_total_project_cost', 'project_tircp_award_amount__$_',
       'project_allocated_amount', 'project_unallocated_amount',
       'project_expended_amount', 'project_award_cycle',
       'project_estimated_tircp_ghg_reductions',
       'project_cost_per_ghg_ton_reduced', 'project_increased_ridership',
       'project_service_integration', 'project_improve_safety',
       'project_project_readiness','project_county']    
'''
Script
'''
def tableau():  
    #Load in cleaned project sheets
    df = A1_data_prep.clean_project()
  
    # Keeping only certain columns.
    df = df[columns_to_keep]

    #Create new cols
    df = df.assign(
    Expended_Percent = (df["project_expended_amount"] / df["project_allocated_amount"]),
    Allocated_Percent = (df["project_allocated_amount"] / df["project_tircp_award_amount__$_"]),
    Unallocated_Amount = (df["project_tircp_award_amount__$_"] - df["project_allocated_amount"]),
    Projects_Funded_Percent = (df['project_tircp_award_amount__$_']/df['project_total_project_cost'])
    )
   
    # filling in for 0's
    new_cols_list = ["Expended_Percent", "Allocated_Percent", "Unallocated_Amount", 'Projects_Funded_Percent'] 
    df[new_cols_list] = df[new_cols_list].fillna(0)
    
    #Replace distircts & counties with their full names 
    df['project_district'] = df['project_district'].replace(crosswalks.full_ct_district)
    df['project_county'] = df['project_county'].replace(crosswalks.full_county)
    
    #Apply functions
    #Categorize projects into expended % bins
    df["Expended_Percent_Group"] = df.apply(lambda x: expended_percent(x), axis=1)
    
    #Categorize projects whether they are ahead/behind/0 expenditures/etc
    df["Progress"] = df.apply(progress, axis=1)
    
    #Categorize projects whether they are large/small/med based on TIRCPamount
    
    #Rename TIRCP column to something cleaner
    df= df.rename(columns={'project_tircp_award_amount__$_': "tircp"})
    # Which projects are large,small, medium
    p75 = df.tircp.quantile(0.75).astype(float)
    p50 = df.tircp.quantile(0.50).astype(float)
    p25 = df.tircp.quantile(0.25).astype(float)
    
    def project_size (row):
        if ((row.tircp > 0) and (row.tircp < p25)):
             return "Small"
        elif ((row.tircp > p25) and (row.tircp < p50)):
             return "Medium"
        elif (row.tircp > p50):
             return "Large"
        elif (row.tircp == 0):
            return "$0 recorded for TIRCP"
        else:
            return "Medium"
        
    df["Project_Category"] = df.apply(lambda x: project_size(x), axis=1)
    
    #Clean up column names
    df.columns = (df.columns
                  .str.replace('[_]', ' ')
                  .str.replace('project','')
                  .str.title()
                  .str.strip()
                 )
    #Write to GCS
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Script_Tableau_Sheet.xlsx") as writer:
        df.to_excel(writer, sheet_name="Data", index=False)
    return df