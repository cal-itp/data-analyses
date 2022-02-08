"""
Data prep

"""
import numpy as np
import pandas as pd

"""
Loading in Data

"""
#GCS File Path:
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/tircp/"

#Project Sheet
def project(): 
    FILE_NAME1 = "Raw_Project_Tracking_Sheet.xlsx"
    df = pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME1}")
    pd.set_option('display.max_columns', None)
    #stripping spaces & _ 
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    #stripping spaces in columns
    df.columns = df.columns.map(lambda x: x.strip())
    # stripping PPNO down to <5 characters
    df = df.assign(PPNO_New = df['PPNO'].str.slice(start=0, stop=5))
     #replacing monetary amounts with 0 
    df[['TIRCP_Award_Amount_($)', 'Allocated_Amount','Expended_Amount','Unallocated_Amount','Total_Project_Cost']] =     df[['TIRCP_Award_Amount_($)','Allocated_Amount','Expended_Amount','Unallocated_Amount','Total_Project_Cost']].fillna(value=0)
    return df

#Allocation Sheet 
def allocation(): 
    FILE_NAME2 = "Allocation_Agreement.xlsx"
    df = pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME2}")
    pd.set_option('display.max_columns', None)
    #stripping spaces & _ 
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    #stripping spaces in columns
    df.columns = df.columns.map(lambda x: x.strip())
    # stripping PPNO down to <5 characters
    df = df.assign(PPNO_New = df['PPNO'].str.slice(start=0, stop=5))
    # replacing missing monetary amounts with 0 
    df["Expended_Amount"].replace({'Deallocation': 0}, inplace=True)
    #rename thid party award date
    df = df.rename(columns = {'3rd_Party_Award_Date':'Third_Party_Award_Date'})
    #clean up dates in a loop
    for i in ["Allocation_Date", "Third_Party_Award_Date", "Completion_Date", "LED"]:
        df[i] = df[i].replace('/', '-', regex = True).replace('Complete', '', regex = True).replace('\n', '', regex=True).replace('Pending','TBD',regex= True).fillna('TBD')
    # coerce to dates
    df = df.assign(
    Allocation_Date_New = pd.to_datetime(df.Allocation_Date, errors="coerce").dt.date,
    Third_Party_Award_Date_New = pd.to_datetime(df.Third_Party_Award_Date, errors="coerce").dt.date,
    Completion_Date_New = pd.to_datetime(df.Completion_Date, errors="coerce").dt.date,
    LED_New = pd.to_datetime(df.LED, errors="coerce").dt.date)
    #dropping old date columns
    df = df.drop(['Allocation_Date','Third_Party_Award_Date','Completion_Date', 'LED'], axis=1)
    #rename coerced columns
    df = df.rename(columns = {'Allocation_Date_New':'Allocation_Date','Third_Party_Award_Date_New':'Third_Party_Award_Date',
    'Completion_Date_New': 'Completion_Date','LED_New': 'LED'})
    #replacing monetary amounts with 0 
    df[['Expended_Amount','Allocation_Amount']] = df[['Expended_Amount','Allocation_Amount']].fillna(value=0)
    return df

"""
Loading in Crosswalks

"""





"""
Misc Functions 

"""
#For table 2 in semi annual report, put projects df in here. 
def summary_SAR_table_two(df):
    #pivot
    df = df.drop_duplicates().groupby(['Award_Year']).agg({'Project_#':'count','TIRCP_Award_Amount_($)':'sum', 
    'Allocated_Amount':'sum','Expended_Amount':'sum'}).reset_index()
    #renaming columns to match report
    df = df.rename(columns = {'Project_#':'Number_of_Awarded_Projects','TIRCP_Award_Amount_($)': 'Award_Amount','Allocated_Amount':'Amount_Allocated'})
    #create percentages
    df['Expended_Percent_of_Awarded'] = (df['Expended_Amount']/df['Award_Amount'])*100
    df['Expended_Percent_of_Allocated'] = (df['Expended_Amount']/df['Amount_Allocated'])*100
    df['Percent_Allocated'] = (df['Amount_Allocated']/df['Award_Amount'])*100
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
    df.at['Expended_Percent_of_Awarded','Grand_Total'] = (Exp/TIRCP)*100
    df.at['Expended_Percent_of_Allocated','Grand_Total'] = (Exp/Alloc)*100
    df.at['Percent_Allocated','Grand_Total'] = (Alloc/TIRCP)*100
    #switching rows to correct order
    df = df.reindex(['Number_of_Awarded_Projects', 'Award_Amount', 'Amount_Allocated','Percent_Allocated','Expended_Amount', 'Expended_Percent_of_Awarded', 'Expended_Percent_of_Allocated'])
    return df 