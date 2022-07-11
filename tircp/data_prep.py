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
   
    #Replace FY 21/22 with Cycle 4
    df["award_cycle"].replace({'FY 21/22': 4}, inplace=True)

    #Coerce cols that are supposed to be numeric
    df['other_funds_involved'] = df['other_funds_involved'].apply(pd.to_numeric, errors='coerce')
    
    #Add prefix
    df = df.add_prefix("project_")
    return df


#Allocation Sheet
def allocation(): 
    df = load_allocation()
    
    #Some rows are not completed: drop them
    df = df.dropna(subset=['award_year', 'grant_recipient', 'ppno'])
    
    #Monetary Columns
    # correcting string to 0 
    df["expended_amount"] = (df["expended_amount"]
                             .replace({'Deallocation': 0})
                             .astype('int64')
                            )
    
    #Fill in NA based on data type
    df = df.fillna(df.dtypes.replace({'float64': 0.0, 'object': 'None'}), inplace=True)
    
    #Dates
    #Replace some string values that are in date columns
    df['_3rd_party_award_date'] = df['_3rd_party_award_date'].replace(crosswalks.allocation_3rd_party_date)
    df['led'] = df['led'].replace(crosswalks.allocation_led)     
    df['completion_date'] = df['completion_date'].replace(crosswalks.allocation_completion_date) 
    
    #Coerce dates to datetime
    date_columns = ['allocation_date', 'completion_date','_3rd_party_award_date', 'led', 
       'date_regional_coordinator_receives_psa', 'date_oc_receives_psa',
       'date_opm_receives_psa', 'date_legal_receives_psa',
       'date_returned_to_pm',
       'date_psa_approved_by_local_agency', 'date_signed_by_drmt',
       'psa_expiry_date','date_branch_chief_receives_psa',]     
    #https://sparkbyexamples.com/pandas/pandas-convert-multiple-columns-to-datetime-type/
    for c in date_columns:
        allocation[c] = allocation[c].apply(pd.to_datetime, errors='coerce')
   
    #Add prefix
    df = df.add_prefix("allocation_")
   
    return df

"""
Semi Annual Report

"""
### SAR ENTIRE REPORT ###
def semi_annual_report():
    ### LOAD IN SHEETS ### 
    df_project = project()
    df_allocation = allocation()
    #Only keeping certain columns
    df_project = df_project[['Project_Manager','Award_Year', 'Project_#','Project_Title','PPNO',
                             'TIRCP_project_sheet','Expended_Amt_project_sheet','Allocated_Amount']]
    df_allocation =(df_allocation[['Expended_Amt_Allocation_Sheet','Allocation_Amt_Allocation_Sheet',
                                   'Award_Year','Award_Recipient', 'Implementing_Agency','PPNO','Phase',      'LED','Allocation_Date','Completion_Date','Third_Party_Award_Date','Components']]
                   )
    
    ###SUMMARY ###
    summary_table_2 = summary_SAR_table_two(df_project) 
    
    ### JOIN ###
    df_sar = df_allocation.merge(df_project, how = "left", on = ["PPNO", "Award_Year"])
    #drop duplicates
    df_sar = df_sar.drop_duplicates() 
    
    ### ADD % ###
    df_sar = df_sar.assign(
    Percent_of_Allocation_Expended = (df_sar['Expended_Amt_Allocation_Sheet']/df_sar['Allocation_Amt_Allocation_Sheet']),
    Percent_of_Award_Fully_Allocated = (df_sar['Allocated_Amount']/df_sar['TIRCP_project_sheet'])
    )
    
    ### CLEAN UP PERCENTS ### 
    cols = ['Expended_Amt_Allocation_Sheet','Allocation_Amt_Allocation_Sheet','TIRCP_project_sheet','Expended_Amt_project_sheet','Percent_of_Allocation_Expended', 'Percent_of_Award_Fully_Allocated']
    df_sar[cols] = df_sar[cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    #rename cols 
    df_sar = df_sar.rename(columns = {'LED': 'Phase_Completion_Date', 'TIRCP_project_sheet': 'TIRCP_Award_Amount','Third_Party_Award_Date':'CON_Contract_Award_Date'})
    
    ### CLEAN DATE-TIME  ### 
    #fill in missing dates with a fake one
    missing_date = pd.to_datetime('2100-01-01')
    dates = ["Allocation_Date", "CON_Contract_Award_Date", "Completion_Date", "Phase_Completion_Date"]
    for i in dates:
        df_sar[i] = df_sar[i].fillna(missing_date)
    #force to date time
    df_sar[dates] = df_sar[dates].apply(pd.to_datetime)
    
    #if the allocation date is AFTER  7-31-2020 then 0, if BEFORE 7-31-2020 then 1
    df_sar = df_sar.assign(Allocated_Before_July_31_2020 = df_sar.apply(lambda x: ' ' if x.Allocation_Date > pd.Timestamp(2020, 7, 31, 0) else 'X', axis=1))
    
    ### PIVOT ### 
    df_pivot =(
    df_sar.groupby(['Award_Year','Project_#','Award_Recipient','Project_Title',
                        'Project_Manager','TIRCP_Award_Amount','Percent_of_Award_Fully_Allocated','Components','PPNO','Phase',"Allocation_Date", 
     "CON_Contract_Award_Date", "Completion_Date", "Phase_Completion_Date", ]).agg({'Allocation_Amt_Allocation_Sheet': 'sum', 
    'Expended_Amt_Allocation_Sheet':'sum',
    'Percent_of_Allocation_Expended':'max',                                                                                                               
    'Allocated_Before_July_31_2020':'max',
    })
    )
    ### GCS ###
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Semi_Annual_Report.xlsx") as writer:
        summary_table_2.to_excel(writer, sheet_name="Summary", index=True)
        df_pivot.to_excel(writer, sheet_name="FY", index=True)
    return df_pivot

#For table 2 in semi annual report
def summary_SAR_table_two(df):
    #pivot
    df = df.drop_duplicates().groupby(['Award_Year']).agg({'Project_#':'count','TIRCP_project_sheet':'sum', 
    'Allocated_Amount':'sum','Expended_Amt_project_sheet':'sum'}).reset_index()
    #renaming columns to match report
    df = (df.rename(columns = {'Project_#':'Number_of_Awarded_Projects',
                               'TIRCP_project_sheet': 'Award_Amount',
                               'Allocated_Amount':'Amount_Allocated',
                               'Expended_Amt_project_sheet': 'Expended_Amount'})
         )
    #create percentages
    df['Expended_Percent_of_Awarded'] = (df['Expended_Amount']/df['Award_Amount'])
    df['Expended_Percent_of_Allocated'] = (df['Expended_Amount']/df['Amount_Allocated'])
    df['Percent_Allocated'] = (df['Amount_Allocated']/df['Award_Amount'])
    #transpose 
    df = df.set_index('Award_Year').T
    #grand totals for monetary columns
    list_to_add = ['Award_Amount','Amount_Allocated','Expended_Amount', 'Number_of_Awarded_Projects']
    df['Grand_Total']=df.loc[list_to_add, :].sum(axis=1)
    #grand total variables of each monetary column to fill in percentages below.
    Exp = df.at['Expended_Amount','Grand_Total']
    Alloc = df.at['Amount_Allocated','Grand_Total']
    TIRCP = df.at['Award_Amount','Grand_Total']
    #filling in totals of percentages
    df.at['Expended_Percent_of_Awarded','Grand_Total'] = (Exp/TIRCP)
    df.at['Expended_Percent_of_Allocated','Grand_Total'] = (Exp/Alloc)
    df.at['Percent_Allocated','Grand_Total'] = (Alloc/TIRCP)
    #switching rows to correct order
    df = (df.reindex(['Number_of_Awarded_Projects', 'Award_Amount', 'Amount_Allocated',
                     'Percent_Allocated','Expended_Amount', 'Expended_Percent_of_Awarded', 'Expended_Percent_of_Allocated'])
    )
    return df 

"""
Program Allocation Plan

"""
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
    

"""
Tableau 

"""
### SHEET CONNECTED TO TABLEAU ### 
def tableau():
    df = project()
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
    
    #Categorizing expended percentage into bins
    def expended_percent(row):
            if row.Expended_Percent == 0:
                return "No expenditure recorded"
            elif ((row.Expended_Percent > 0) and (row.Expended_Percent < .50)):
                return "1-50"
            elif row.Expended_Percent < 0.71:
                return "51-70"
            else:
                return "71-100"
    df["Expended_Percent_Group"] = df.apply(lambda x: expended_percent(x), axis=1)
    
    # Categorize years and expended_percent_group into bins
    def progress(df):   
        if (df['Award_Year'] == 2015) and (df['Expended_Percent_Group'] == "1-50"):
            return 'Behind'
        elif (df['Award_Year'] == 2015) and (df['Expended_Percent_Group'] == "51-70"):
            return 'On Track'
        elif (df['Award_Year'] == 2015) and (df['Expended_Percent_Group'] == "71-100"):
            return 'On Track'
        elif (df['Award_Year'] == 2016) and (df['Expended_Percent_Group'] == "1-50"):
            return 'Behind'
        elif (df['Award_Year'] == 2016) and (df['Expended_Percent_Group'] == "71-100"):
            return 'On Track'
        elif (df['Award_Year'] == 2016) and (df['Expended_Percent_Group'] == "51-70"):
            return 'On Track'
        elif (df['Award_Year'] == 2018) and (df['Expended_Percent_Group'] == "1-50"):
            return 'On Track'
        elif (df['Award_Year'] == 2018) and (df['Expended_Percent_Group'] == "51-70"):
            return 'Ahead'
        elif (df['Award_Year'] == 2018) and (df['Expended_Percent_Group'] == "71-100"):
            return 'Ahead'
        elif (df['Award_Year'] == 2020) and (df['Expended_Percent_Group'] == "1-50"):
            return 'On Track'
        elif (df['Award_Year'] == 2020) and (df['Expended_Percent_Group'] == "51-70"):
            return 'Ahead'
        elif (df['Award_Year'] == 2020) and (df['Expended_Percent_Group'] == "71-100"):
            return 'Ahead'
        else: 
            return "No Expenditures"

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
        
    df["Project_Category"] = df.apply(lambda x: project_size(x), axis=1)
     #Rename cols to the right names
    df = (df.rename(columns = {'Expended_Amt_project_sheet':'Expended_Amount', 
                                                'TIRCP_project_sheet': "TIRCP_Amount"}
                  ))
    ### GCS ###
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Tableau_Sheet.xlsx") as writer:
        df.to_excel(writer, sheet_name="Data", index=False)
    return df

    ### GCS ###
    df = df.to_parquet(f'{GCS_FILE_PATH}TIRCP_Tableau_Parquet.parquet')
    return df 
    return df 
