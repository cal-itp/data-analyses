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
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    df.columns = df.columns.map(lambda x: x.strip())
    
    ### PPNO CLEAN UP ###
    # stripping PPNO down to <5 characters
    df = df.assign(PPNO_New = df['PPNO'].str.slice(start=0, stop=5))
    
    ### RECIPIENTS ###
    #Some grant recipients have multiple spellings of their name. E.g. BART versus Bay Area Rapid Tranist
    df['Local_Agency'] = (df['Local_Agency'].replace({'San Joaquin Regional\nRail Commission / San Joaquin Joint Powers Authority':
                                                      'San Joaquin Regional Rail Commission / San Joaquin Joint Powers Authority', 
                                                 'San Francisco Municipal  Transportation Agency':'San Francisco Municipal Transportation Agency',
                                                 'San Francisco Municipal Transportation Agency (SFMTA)': 'San Francisco Municipal Transportation Agency',
                                                 'Capitol Corridor Joint Powers Authority (CCJPA)':  'Capitol Corridor Joint Powers Authority',
                                                 'Bay Area Rapid Transit (BART)': 'Bay Area Rapid Transit District (BART)',
                                                 'Los Angeles County Metropolitan Transportation Authority (LA Metro)': 'Los Angeles County Metropolitan Transportation Authority',
                                                 'Santa Clara Valley Transportation Authority (SCVTA)': 'Santa Clara Valley Transportation Authority',
                                                 'Solano Transportation Authority (STA)':  'Solano Transportation Authority',
                                                 'Southern California Regional Rail Authority (SCRRA - Metrolink)': 'Southern California  Regional Rail Authority'
                                                 }))
    
    ### CROSSWALK ### 
    df = pd.merge(df, project_ppno_crosswalk, on = ["Award_Year", "Local_Agency"], how = "left")
    df.PPNO_New = df.apply(lambda x: x.PPNO_New if (str(x.PPNO_New2) == 'nan') else x.PPNO_New2, axis=1)
    df = df.drop(['PPNO','PPNO_New2'], axis=1).rename(columns = {'PPNO_New':'PPNO'})
    #Change  PPNO to all be strings
    df.PPNO = df.PPNO.astype(str)
    
    ### MONETARY COLS CLEAN UP ###
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
    
    ### PPNO CLEAN UP ### 
    # stripping PPNO down to <5 characters
    df = df.assign(PPNO_New = df['PPNO'].str.slice(start=0, stop=5))
    #Merge in Crosswalk 
    df = pd.merge(df, allocation_ppno_crosswalk, on = ["Award_Year", "Award_Recipient"], how = "left")
    #Map Crosswalk 
    df.PPNO_New = df.apply(lambda x: x.PPNO_New if (str(x.PPNO_New2) == 'nan') else x.PPNO_New2, axis=1)
    #Drop old PPNO 
    df = df.drop(['PPNO','PPNO_New2'], axis=1).rename(columns = {'PPNO_New': 'PPNO'}) 
    
    ### DATES CLEAN UP ###
    #rename thid party award date
    df = df.rename(columns = {'3rd_Party_Award_Date':'Third_Party_Award_Date'})
    #clean up dates in a loop
    alloc_dates = ["Allocation_Date", "Third_Party_Award_Date", "Completion_Date", "LED",
                  ]
    for i in [alloc_dates]:
        df[i] = (df[i].replace('/', '-', regex = True).replace('Complete', '', regex = True)
            .replace('\n', '', regex=True).replace('Pending','TBD',regex= True)
            .fillna('TBD')
        )
    #replacing values for date columns to be coerced later 
    df["Allocation_Date"] = (df["Allocation_Date"].replace({"FY 26/27": "2026-12-31", "08/12//20": '2020-08-12 00:00:00', 
                                         'FY 21/22': '2021-12-31','FY 22/23': '2022-12-31',
                                         'FY 20/21': '2020-12-31', 'FY 23/24': '2023-12-31',
                                         'FY 24/25': '2024-12-31','FY 25/26': '2025-12-31'})) 
   
    df["Completion_Date"] = (df["Completion_Date"].replace({ 'June 24. 2024': '2024-06-01 00:00:00',  
    '11/21/2024\n7/30/2025 (Q4)': '2024-11-21 00:00:00', 
    'Jun-26': '2026-01-01 00:00:00', 
     'Jun-29': '2029-06-01 00:00:00',
    'Complete\n11/12/2019': '2019-11-12 00:00:00' , 
    'Deallocated': '', 
    'Jun-28': '2028-06-01 00:00:00',  
    'Jun-25': '2025-06-01 00:00:00', 
    'Jun-23':'2023-06-01 00:00:00', 
    'Jun-27': '2027-06-01 00:00:00',
    'Jan-25': '2025-01-01 00:00:00',
    '11-21-20247-30-2025 (Q4)':'2025-07-30 00:00:00',
    '6-30-202112-31-2021': '2021-12-31 00:00:00',
    '6-1-2019': '2019-06-01 00:00:00',
    '2-11-2018': '2018-02-11 00:00:00',
     '6-30-2020': '2020-06-30 00:00:00',
    ' 6-30-2018': '2018-06-30 00:00:00',
     '6-29-2020': '2020-06-29 00:00:00',
     '11-1-2019': '2019-11-01 00:00:00',
     ' 12-10-2018': '2018-12-10 00:00:00',
     ' 11-13-2019': '2019-11-13 00:00:00',
     '3-30-2020':'2020-03-30 00:00:00',
    ' 6-30-2020': '2020-06-30 00:00:00',
    '11-12-2019': '2019-11-12 00:00:00',
    '1-31-2020': '2020-01-31 00:00:00',
    '8-30-2020': '2020-08-30 00:00:00',
    '5-16-2020': '2020,05-16 00:00:00',
     '5-7-2020': '2020-05-07 00:00:00'})) 
    
    df["Third_Party_Award_Date"] = df["Third_Party_Award_Date"].replace({ 
    'Augsut 12, 2021': '2021-08-12 00:00:00',
    '43435': '2018-12-01 00:00:00',
    '07-29-2020': '2020-07-29 00:00:00',
    '43497' : '2019-02-01 00:00:00',
    'TBD 6-24-2021' : '2021-06-24 00:00:00',
    'TBD 6-30-2022' : '2022-06-30 00:00:00'})
    
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
    
    ### CLEAN UP MONETARY COLS ###
    # correcting string to 0 
    df["Expended_Amount"].replace({'Deallocation': 0}, inplace=True)
    #replacing monetary amounts with 0 & coerce to numeric 
    allocation_monetary_cols = ['SB1_Funding','Expended_Amount','Allocation_Amount',
       'GGRF_Funding','Prior_Fiscal_Years_to_2020',
       'Fiscal_Year_2020-2021', 'Fiscal_Year_2021-2022',
       'Fiscal_Year_2022-2023', 'Fiscal_Year_2023-2024',
       'Fiscal_Year_2024-2025', 'Fiscal_Year_2025-2026',
       'Fiscal_Year_2026-2027', 'Fiscal_Year_2027-2028',
       'Fiscal_Year_2028-2029', 'Fiscal_Year_2029-2030']
    df[allocation_monetary_cols] = df[allocation_monetary_cols].fillna(value=0)
    df[allocation_monetary_cols] = df[allocation_monetary_cols].apply(pd.to_numeric, errors='coerce')
    #rename columns that are similar to project sheet to avoid confusion
    df = (df.rename(columns = {'Allocation_Amount':'Allocation_Amt_Allocation_Sheet',
                               'Expended_Amount': 'Expended_Amt_Allocation_Sheet'})
         )
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
    with pd.ExcelWriter(f"{GCS_FILE_PATH}FUNCTION_TEST_TIRCP_SAR.xlsx") as writer:
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
    with pd.ExcelWriter(f'{GCS_FILE_PATH}FUNCTION_TEST_PAP.xlsx') as writer:
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
    #Keeping only the columns we want
    df = project() 
    
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
    df.to_csv(f'{GCS_FILE_PATH}df_tableau_sheet.csv', index = False)
    return df 

'''
CHARTS
'''
#Labels
def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    LABEL_DICT = { "prepared_y": "Year",
              "dist": "District",
              "nunique":"Number of Unique",
              "project_no": "Project Number"}
    
    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    elif word in LABEL_DICT.keys():
        word = LABEL_DICT[word]
    else:
        #word = word.replace('n_', 'Number of ').title()
        word = word.replace('unique_', "Number of Unique ").title()
        word = word.replace('_', ' ').title()
    
    return word

# Bar
def basic_bar_chart(df, x_col, y_col, colorcol):
    
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), sort=('-y')),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 color = alt.Color(colorcol,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_CATEGORY_BOLD_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ))
             .properties( 
                          title=(f"{labeling(x_col)} by {labeling(y_col)}"))
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./Charts/bar_{x_col}_by_{y_col}.png")
    return chart


# Scatter 
def basic_scatter_chart(df, x_col, y_col, colorcol):
    
    chart = (alt.Chart(df)
             .mark_circle(size=350)
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 color = alt.Color(colorcol,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_CATEGORY_BOLD_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ))
             .properties( 
                          title = (f"{labeling(x_col)} by {labeling(y_col)}"))
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./Charts/scatter_{x_col}_by_{y_col}.png")
    return chart

