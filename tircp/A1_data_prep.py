"""
Data prep
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

"""
Functions
"""
#Some PPNO numbers are 5+. Slice them down to <= 5.
def ppno_slice(df):
    df = df.assign(ppno = df['ppno'].str.slice(start=0, stop=5))
    return df 

"""
Import the Data
"""
#Project Sheet
def load_project(): 
    #Load in 
    df = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME}", sheet_name="Project Tracking"))
    #Clean PPNO, strip down to <5 characters
    df = ppno_slice(df)
    return df

#Allocation Agreement Sheet
def load_allocation(): 
    #Load in 
    df =  to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME}", sheet_name="Agreement Allocations"))
    #Clean PPNO, strip down to <5 characters
    df = ppno_slice(df)
    return df

#Previous SAR - this one is fake
def load_previous_sar():
    file_to_sar = "Fake_SAR_4_14.xlsx"
    df = pd.read_excel(f"{GCS_FILE_PATH}{file_to_sar}")
    return df
'''
Clean Up the Data:
'''
#Clean up project sheet 
def clean_project():
    df = load_project()
    #Replace some values manually that are NaN
    df.loc[(df["grant_recipient"] == "San Bernardino County Transportation Authority (SBCTA)"), "ppno"] = '1230'
    df.loc[(df["grant_recipient"] == "Bay Area Rapid Transit District (BART)"), "ppno"] = 'CP060'
    df.loc[(df["grant_recipient"] == "Santa Monica Big Blue Bus"), "ppno"] = 'CP071'
    
    #Replace grant recipients
    #Some grant recipients have multiple spellings of their name. E.g. BART versus Bay Area Rapid Tranist
    df['grant_recipient'] = df['grant_recipient'].replace(crosswalks.grant_recipients_projects)
   
    #Fill in nulls based on data type
    df = df.fillna(df.dtypes.replace({'float64': 0.0, 'object': 'None', 'int64':0})) 
    
    #Replace FY 21/22 with Cycle 4
    df["award_cycle"].replace({'FY 21/22': 4}, inplace=True)

    #Coerce cols that are supposed to be numeric
    df['other_funds_involved'] = df['other_funds_involved'].apply(pd.to_numeric, errors='coerce')
    
    #Add prefix
    df = df.add_prefix("project_")
    return df


#Allocation Sheet
def clean_allocation(): 
    df = load_allocation()
    
    #Coerce dates to datetime
    date_columns = ['allocation_date', 'completion_date','_3rd_party_award_date', 'led', 
       'date_regional_coordinator_receives_psa', 'date_oc_receives_psa',
       'date_opm_receives_psa', 'date_legal_receives_psa',
       'date_returned_to_pm',
       'date_psa_approved_by_local_agency', 'date_signed_by_drmt',
       'psa_expiry_date','date_branch_chief_receives_psa',]     
    
    #Some rows are not completely filled: drop them
    df = df.dropna(subset=['award_year', 'grant_recipient', 'ppno'])
    
    #Dates
    #Replace some string values that are in date columns
    df['_3rd_party_award_date'] = df['_3rd_party_award_date'].replace(crosswalks.allocation_3rd_party_date)
    df['led'] = df['led'].replace(crosswalks.allocation_led)     
    df['completion_date'] = df['completion_date'].replace(crosswalks.allocation_completion_date) 
    
    #Monetary Columns
    # correcting string to 0 
    df["expended_amount"] = (df["expended_amount"]
                             .replace({'Deallocation': 0})
                             .astype('int64')
                            )
    
    #Fill in NA based on data type
    df = df.fillna(df.dtypes.replace({'float64': 0.0, 'object': 'None'}))
    
    #https://sparkbyexamples.com/pandas/pandas-convert-multiple-columns-to-datetime-type/
    for c in date_columns:
        df[c] = df[c].apply(pd.to_datetime, errors='coerce')
   
    #Add prefix
    df = df.add_prefix("allocation_")
   
    return df

'''
Program Allocation Plan
'''
def program_allocation_plan(): 
    ### LOAD IN SHEETS ### 
    df_project = project()
    df_allocation = allocation()
    #Only keeping certain columns
    df_project = (df_project[['Award_Year', 'Project_#','TIRCP_project_sheet','Local_Agency','Project_Title',
                              'PPNO', 'Unallocated_amt_project_sheet']]
                 )
    df_allocation = (df_allocation[['Award_Year','Award_Recipient', 'Implementing_Agency',
    'Components', 'PPNO','Phase',
    'Fiscal_Year_2020-2021', 'Fiscal_Year_2021-2022',
    'Fiscal_Year_2022-2023', 'Fiscal_Year_2023-2024',
    'Fiscal_Year_2024-2025', 'Fiscal_Year_2025-2026',
    'Fiscal_Year_2026-2027', 'Fiscal_Year_2027-2028',
    'Fiscal_Year_2028-2029', 'Fiscal_Year_2029-2030','CTC_Financial_Resolution',
    'Allocation_Date','Project_ID','SB1_Funding','GGRF_Funding','Allocation_Amt_Allocation_Sheet']]
                    ) 
    ### MERGE 2 SHEETS ###
    df_combined = df_allocation.merge(df_project, how = "left", on = ["PPNO", "Award_Year"])
    
    ### CLEAN UP ###
    #Fill in Project ID & CTC Fin Resolution with TBD so it'll show up
    df_combined[['Project_ID','CTC_Financial_Resolution']] = (df_combined[['Project_ID',
                                                            'CTC_Financial_Resolution']].fillna(value = 'TBD'))
    #Fill in missing dates with something random 
    missing_date = pd.to_datetime('2100-01-01')
    df_combined['Allocation_Date'] = df_combined['Allocation_Date'].fillna(missing_date)
    
    #Create Total_Amount Col
    df_combined['Total_Amount'] = df_combined['GGRF_Funding'] + df_combined['SB1_Funding']
    
    #Rename cols to the right names
    df_combined = (df_combined.rename(columns = {'TIRCP_project_sheet':'Award_Amount', 
                                                'Components': "Separable_Phases/Components",
                                                'CTC_Financial_Resolution': 'Allocation_Resolution',
                                                'SB1_Funding':'PTA-SB1_Amount', 
                                                'Unallocated_amt_project_sheet': 'Not_Allocated'})
                  ) 
    
    ### PIVOT ### 
    def pivot(df):
        df = df.groupby(['Award_Year','Project_#','Award_Amount','Not_Allocated','PPNO','Award_Recipient','Implementing_Agency',
        'Project_Title', 'Separable_Phases/Components','Phase','Project_ID','Allocation_Resolution','Allocation_Date']).agg({
        'Fiscal_Year_2020-2021': 'max',
        'Fiscal_Year_2021-2022': 'max', 'Fiscal_Year_2022-2023': 'max',
        'Fiscal_Year_2023-2024': 'max', 'Fiscal_Year_2024-2025': 'max',
        'Fiscal_Year_2025-2026': 'max', 'Fiscal_Year_2026-2027': 'max',
        'Fiscal_Year_2027-2028': 'max', 'Fiscal_Year_2028-2029': 'max',
        'Fiscal_Year_2029-2030': 'max', 'PTA-SB1_Amount': 'sum', 'GGRF_Funding':'sum',
         'Total_Amount':'sum'})
        return df 
    
    df_2015 = pivot(df_combined.loc[df_combined['Award_Year'] == 2015])
    df_2016 = pivot(df_combined.loc[df_combined['Award_Year'] == 2016])
    df_2018 = pivot(df_combined.loc[df_combined['Award_Year'] == 2018])
    df_2020 = pivot(df_combined.loc[df_combined['Award_Year'] == 2020])
    
    #GCS 
    with pd.ExcelWriter(f'{GCS_FILE_PATH}Program_Allocation_Plan.xlsx') as writer:
        df_2015.to_excel(writer, sheet_name="2015_Cycle_1", index=True)
        df_2016.to_excel(writer, sheet_name="2016_Cycle_2", index=True)
        df_2018.to_excel(writer, sheet_name="2018_Cycle_3", index=True)
        df_2020.to_excel(writer, sheet_name="2020_Cycle_4", index=True)
        
    return df_combined
    

