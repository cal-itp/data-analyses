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
import data_prep

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
    elif (row.Expended_Percent > 0.75) and (row.Expended_Percent < 1.0):
        return "76-99"
    elif row.Expended_Percent == 0.0:
         return "0"
    else:
        return "100"
    
# Categorize years and expended_percent_group into bins
def progress(df):
    ### 2015 ###
    if (df["Award_Year"] == 2015) and (df["Expended_Percent_Group"] == "1-25") | (
            df["Expended_Percent_Group"] == "26-50"
        ):
           return "Behind"
    elif (df["Award_Year"] == 2015) and (
            df["Expended_Percent_Group"] == "76-99"
        ) | (df["Expended_Percent_Group"] == "51-75"):
         return "On Track"

     ### 2016 ###
    elif (df["Award_Year"] == 2016) and (df["Expended_Percent_Group"] == "1-25") | (
            df["Expended_Percent_Group"] == "26-50"
        ):
        return "Behind"
    elif (df["Award_Year"] == 2016) and (
            df["Expended_Percent_Group"] == "51-75"
        ) | (df["Expended_Percent_Group"] == "76-99"):
         return "On Track"

     ### 2018 ###
    elif (df["Award_Year"] == 2018) and (df["Expended_Percent_Group"] == "1-25"):
        return "Behind"
    elif (df["Award_Year"] == 2018) and (
            df["Expended_Percent_Group"] == "26-50"
        ) | (df["Expended_Percent_Group"] == "51-75"):
        return "On Track"
    elif (df["Award_Year"] == 2018) and (df["Expended_Percent_Group"] == "76-99"):
         return "Ahead"

     ### 2020 ###
    elif (df["Award_Year"] == 2020) and (df["Expended_Percent_Group"] == "1-25"):
           return "Behind"
    elif (df["Award_Year"] == 2020) and (df["Expended_Percent_Group"] == "26-50"):
           return "On Track"
    elif (df["Award_Year"] == 2020) and (
            df["Expended_Percent_Group"] == "51-75"
        ) | (df["Expended_Percent_Group"] == "76-99"):
            return "Ahead"

    ### 0 Expenditures ###
    elif df["Expended_Percent_Group"] == "0":
        return "No expenditures recorded"

    ### Else ###
    else:
        return "100% of allocated funds spent"
        
#Which projects are large,small, medium
p75 = df.TIRCP_project_sheet.quantile(0.75).astype(float)
p25 = df.TIRCP_project_sheet.quantile(0.25).astype(float)
p50 = df.TIRCP_project_sheet.quantile(0.50).astype(float)
    
def project_size (row):
    if ((row.TIRCP_project_sheet > 0) and (row.TIRCP_project_sheet < p25)):
         return "Small"
    elif ((row.TIRCP_project_sheet > p25) and (row.TIRCP_project_sheet < p75)):
         return "Medium"
    elif ((row.TIRCP_project_sheet > p50) and (row.TIRCP_project_sheet > p75 )):
         return "Large"
    else:
        return "$0 recorded for TIRCP"
        
'''
Script
'''
def tableau():
    df = data_prep.project()
    #Keeping only the columns we want
    df = (df[['PPNO','Award_Year', 'Project_#', 'Local_Agency', 'Vendor_ID_#',
       'Project_Title', 'District', 'County', 'Key_Project_Elements',
       'Master_Agreement_Number', 'Master_Agreement_Expiration_Date',
       'Project_Manager', 'Regional_Coordinator',
       'Technical_Assistance-CALTP_(Y/N)', 'Technical_Assistance-Fleet_(Y/N)',
       'Technical_Assistance-Network_Integration_(Y/N)',
       'Technical_Assistance-Priority_Population_(Y/N)', 'Total_Project_Cost',
       'TIRCP_project_sheet', 'Allocated_Amount',
       'Unallocated_amt_project_sheet', 'Percentge_Allocated',
       'Expended_Amt_project_sheet', 'Other_Funds_Involved']]
                 )
    #Getting percentages & filling in with 0
    df['Expended_Percent'] = df['Expended_Amt_project_sheet']/df['Allocated_Amount']
    df['Allocated_Percent'] = df['Allocated_Amount']/df['TIRCP_project_sheet']
    df[['Expended_Percent','Allocated_Percent']] = df[['Expended_Percent','Allocated_Percent']].fillna(value=0)
    
 
    df["Expended_Percent_Group"] = df.apply(lambda x: expended_percent(x), axis=1)
    
  
    df['Progress'] = df.apply(progress, axis = 1)
    
    #Renaming districts
    df['District'] = (df['District'].replace({7:'District 7: Los Angeles',
                                            4:'District 4: Bay Area / Oakland',
                                            'VAR':'Various',
                                            10:'District 10: Stockton',
                                            11:'District 11: San Diego',
                                            3:'District 3: Marysville / Sacramento',
                                            12: 'District 12: Orange County',
                                            8: 'District 8: San Bernardino / Riverside',
                                            5:'District 5: San Luis Obispo / Santa Barbara',
                                            6:'District 6: Fresno / Bakersfield',
                                            1:'District 1: Eureka'
                                                 }))
    #Renaming counties
    df['County'] = (df['County'].replace({'LA': 'Los Angeles', 
                                          'VAR': 'Various', 
                                          'MON': 'Mono', 
                                          'ORA':'Orange', 
                                          'SAC':'Sacramento', 
                                          'SD':'San Diego', 
                                          'SF': 'San Francisco', 
                                          'SJ':'San Joaquin', 
                                          'SJ ': 'San Joaquin', 
                                          'FRE':"Fresno",
                                           'SBD':'San Bernandino', 
                                          'SCL':'Santa Clara', 
                                          'ALA':'Alameda', 
                                          'SM':'San Mateo', 
                                          'SB': 'San Barbara', 
                                          'SON, MRN': 'Various', 

                                                 }))
    
    df["Project_Category"] = df.apply(lambda x: project_size(x), axis=1)
     #Rename cols to the right names
    df = (df.rename(columns = {'Expended_Amt_project_sheet':'Expended_Amount', 
                                                'TIRCP_project_sheet': "TIRCP_Amount"}
                  ))
    ### GCS ###
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Tableau_Sheet.xlsx") as writer:
        df.to_excel(writer, sheet_name="Data", index=False)
    return df 

