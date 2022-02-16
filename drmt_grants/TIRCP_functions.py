"""
Data prep

"""

import numpy as np
import pandas as pd

"""
Loading in Crosswalks

"""
#GCS File Path:
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/tircp/"

#Allocation PPNO Crosswalk
FILE_NAME3 = "Allocation_PPNO_Crosswalk.csv"
allocation_ppno_crosswalk = pd.read_csv(f"{GCS_FILE_PATH}{FILE_NAME3}")
    
#Allocation PPNO Crosswalk
FILE_NAME4 = "Projects_PPNO.xlsx"
project_ppno_crosswalk = pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME4}")

"""
Cleaning & Loading Data

"""

#Project Sheet
def project(): 
    FILE_NAME1 = "Raw_Project_Tracking_Sheet.xlsx"
    df = pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME1}")
    #stripping spaces & _ 
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    #stripping spaces in columns
    df.columns = df.columns.map(lambda x: x.strip())
    # stripping PPNO down to <5 characters
    df = df.assign(PPNO_New = df['PPNO'].str.slice(start=0, stop=5))
    #replacing monetary amounts with 0 
    proj_cols = ['TIRCP_Award_Amount_($)', 'Allocated_Amount','Expended_Amount','Unallocated_Amount','Total_Project_Cost','Other_Funds_Involved']
    df[proj_cols] = df[proj_cols].fillna(value=0)
    df[proj_cols] = df[proj_cols].apply(pd.to_numeric, errors='coerce')
    #rename to avoid confusion with allocation sheet
    df = (df.rename(columns = {'TIRCP_Award_Amount_($)':'TIRCP_project_sheet',
                               'Expended_Amount': 'Expended_Amt_project_sheet',
                               'Unallocated_Amount':'Unallocated_amt_project_sheet'})
         )
    return df

#Allocation Sheet
def allocation(): 
    FILE_NAME2 = "Allocation_Agreement.xlsx"
    df = pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME2}")
    #stripping spaces & _ 
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    #stripping spaces in columns
    df.columns = df.columns.map(lambda x: x.strip())
    # stripping PPNO down to <5 characters
    df = df.assign(PPNO_New = df['PPNO'].str.slice(start=0, stop=5))
    # correcting string to 0 
    df["Expended_Amount"].replace({'Deallocation': 0}, inplace=True)
    #rename thid party award date
    df = df.rename(columns = {'3rd_Party_Award_Date':'Third_Party_Award_Date'})
    #clean up dates in a loop
    alloc_dates = ["Allocation_Date", "Third_Party_Award_Date", "Completion_Date", "LED"]
    for i in [alloc_dates]:
        df[i] = (df[i].replace('/', '-', regex = True).replace('Complete', '', regex = True)
            .replace('\n', '', regex=True).replace('Pending','TBD',regex= True)
            .fillna('TBD')
        )
    
    # coerce to dates
    df = df.assign(
    Allocation_Date_New = pd.to_datetime(df.Allocation_Date, errors="coerce").dt.date,
    Third_Party_Award_Date_New = pd.to_datetime(df.Third_Party_Award_Date, errors="coerce").dt.date,
    Completion_Date_New = pd.to_datetime(df.Completion_Date, errors="coerce").dt.date,
    LED_New = pd.to_datetime(df.LED, errors="coerce").dt.date)
    #dropping old date columns
    df = df.drop(alloc_dates, axis=1)
    #rename coerced columns
    df = (df.rename(columns = {'Allocation_Date_New':'Allocation_Date',
                               'Third_Party_Award_Date_New':'Third_Party_Award_Date',
                               'Completion_Date_New': 'Completion_Date','LED_New': 'LED'})
         )
    #replacing monetary amounts with 0 & coerce to numeric 
    allocation_monetary = ['SB1_Funding','Expended_Amount','Allocation_Amount',
       'GGRF_Funding','Prior_Fiscal_Years_to_2020',
       'Fiscal_Year_2020-2021', 'Fiscal_Year_2021-2022',
       'Fiscal_Year_2022-2023', 'Fiscal_Year_2023-2024',
       'Fiscal_Year_2024-2025', 'Fiscal_Year_2025-2026',
       'Fiscal_Year_2026-2027', 'Fiscal_Year_2027-2028',
       'Fiscal_Year_2028-2029', 'Fiscal_Year_2029-2030']
    df[allocation_monetary] = df[allocation_monetary].fillna(value=0)
    df[allocation_monetary] = df[allocation_monetary].apply(pd.to_numeric, errors='coerce')
    #rename columns that are similar to project sheet to avoid confusion
    df = (df.rename(columns = {'Allocation_Amount':'Allocation_Amt_Allocation_Sheet',
                               'Expended_Amount': 'Expended_Amt_Allocation_Sheet'})
         )
    return df



"""
Semi Annual Report

"""
#For table 2 in semi annual report, put projects df in here
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
Tableau Functions

"""
#Categorizing expended percentage for Tableau dashboard
#Input project dataframe here
def expended_percent(row):
            if row.Expended_Percent == 0:
                return "No expenditure recorded"
            elif ((row.Expended_Percent > 0) and (row.Expended_Percent < .50)):
                return "1-50"
            elif row.Expended_Percent < 0.71:
                return "51-70"
            else:
                return "71-100"
#df["Expended_Percent_Group"] = df.apply(lambda x: expended_percent(x), axis=1)

#Categorizing expended percentage & cycle for Tableau dashboard
#Input project dataframe here
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
#df['Progress'] = df.apply(progress, axis = 1)