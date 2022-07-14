'''
Semiannual Report
'''
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
#Function for the summary table 
def summary_SAR_table_two(df):
    #pivot
    df = (df
          .drop_duplicates()
          .groupby(['project_award_year'])
          .agg({'project_project_#':'count',
                'project_tircp_award_amount__$_':'sum', 
                'project_allocated_amount':'sum',
                'project_expended_amount':'sum'})
          .reset_index()
         )
    #renaming columns to match report
    df = (df.rename(columns = {'project_project_#':'Number_of_Awarded_Projects',
                               'project_tircp_award_amount__$_': 'Award_Amount',
                               'project_allocated_amount':'Amount_Allocated',
                               'project_expended_amount': 'Expended_Amount',
                               'project_award_year': 'Award_Year'})
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
    #grand total project_expended_amount of each monetary column to fill in percentages below.
    Exp = df.at['Expended_Amount','Grand_Total']
    Alloc = df.at['Amount_Allocated','Grand_Total']
    TIRCP = df.at['Award_Amount','Grand_Total']
    #filling in totals of percentages
    df.at['Expended_Percent_of_Awarded','Grand_Total'] = (Exp/TIRCP)
    df.at['Expended_Percent_of_Allocated','Grand_Total'] = (Exp/Alloc)
    df.at['Percent_Allocated','Grand_Total'] = (Alloc/TIRCP)
    #switching rows to correct order
    df = (df.reindex(['Number_of_Awarded_Projects',
                      'Award_Amount', 'Amount_Allocated',
                     'Percent_Allocated','Expended_Amount', 
                      'Expended_Percent_of_Awarded', 'Expended_Percent_of_Allocated'])
    )
    return df 
'''
Columns
&
Lists
'''
#Columns to keep 
allocation_cols = ['allocation_award_year','allocation_expended_amount','allocation_allocation_amount',
                                'allocation_components','allocation_grant_recipient', 
                                   'allocation_implementing_agency','allocation_ppno',
                                   'allocation_phase','allocation_led','allocation_allocation_date',
                                   'allocation_completion_date','allocation__3rd_party_award_date',
                                  'allocation_ea', 'allocation_sb1_funding',  'allocation_ggrf_funding',
                                   'allocation_project_id']
project_cols = ['project_project_manager','project_award_year', 'project_project_#','project_project_title',
                             'project_ppno',  'project_tircp_award_amount__$_',
                             'project_expended_amount','project_allocated_amount','project_grant_recipient']

numeric_cols = ['allocation_expended_amount','allocation_allocation_amount',
            'project_tircp_award_amount__$_','project_expended_amount',
            'Percent_of_Allocation_Expended', 'Percent_of_Award_Fully_Allocated']

#Date Columns 
dates = ["allocation_allocation_date", "allocation__3rd_party_award_date",
             "allocation_completion_date", "allocation_led"]

#Columns for aggregating
group_by_cols = ['project_award_year','project_project_#','project_project_manager',
                 'allocation_grant_recipient', 'allocation_implementing_agency',
                 'project_project_title', 'Percent_of_Award_Fully_Allocated','TIRCP_Award_Amount',
                 'allocation_components','project_ppno','allocation_phase',
                 "allocation_allocation_date",  "CON_Contract_Award_Date",
                 "Phase_Completion_Date", 'allocation_ea', 'allocation_project_id',]

sum_cols = ['allocation_allocation_amount','allocation_expended_amount','allocation_sb1_funding',
    'allocation_ggrf_funding']

max_cols = ['Percent_of_Allocation_Expended','Allocated_Before_July_31_2020']

'''
Complete Semiannual Report
'''
def full_sar_report():
    #Load in raw sheets
    df_project = A1_data_prep.clean_project()
    df_allocation = A1_data_prep.clean_allocation()
    previous_sar = A1_data_prep.load_previous_sar()
    
    #Function for summary table portion of the report
    summary = summary_SAR_table_two(df_project)
    
    #Only keeping certain columns
    df_project = (df_project[project_cols])
    df_allocation =(df_allocation[allocation_cols])
    
    #Join the 2 dataframes
    m1 = df_allocation.merge(df_project, how = "left", 
                                 left_on = ["allocation_ppno", "allocation_award_year"],
                                 right_on = ["project_ppno", "project_award_year"])
    #drop duplicates
    m1 = m1.drop_duplicates() 
    
    #Fill in missing dates with a fake one so it'll show up in the group by 
    missing_date = pd.to_datetime('2100-01-01')
    for i in dates:
        m1[i] = (m1[i]
                     .fillna(missing_date)
                     .apply(pd.to_datetime)
                    )
    
    #Add new columns with percentages and a new column to flag whether an allocation date is 
    #AFTER  7-31-2020 then blank, if BEFORE 7-31-2020 then X
    m1 = m1.assign(
    Percent_of_Allocation_Expended = (m1['allocation_expended_amount']/
                                      m1['allocation_allocation_amount']),
    Percent_of_Award_Fully_Allocated = (m1['allocation_allocation_amount']/
                                        m1['project_tircp_award_amount__$_']),
    Allocated_Before_July_31_2020 =   m1.apply(lambda x: ' ' if x.allocation_allocation_date 
                                        > pd.Timestamp(2020, 7, 31, 0) else 'X', axis=1))
    
        
    #Filter out projects that are excluded 
    m1 = (m1[(m1.allocation_allocation_amount > 0 ) & (m1.Percent_of_Allocation_Expended < 0.99)]) 
    
    #Fill in null values based on datatype of each column
    m1 = m1.fillna(m1.dtypes.replace({'float64': 0.0, 'int64': 0}))
    
    #Rename cols 
    m1 = m1.rename(columns = {'allocation_led': 'Phase_Completion_Date',
         'project_tircp_award_amount__$_': 'TIRCP_Award_Amount',
         'allocation__3rd_party_award_date':'CON_Contract_Award_Date'})

    #Pivot
    df_pivoted =m1.groupby(group_by_cols).agg({**{e:'max' for e in max_cols}, **{e:'sum' for e in sum_cols}})
    
    #Apply styling to show difference between current SAR and previous SAR
    #https://stackoverflow.com/questions/17095101/compare-two-dataframes-and-output-their-differences-side-by-side
    #Reset index from dataframe above
    df_current = df_pivoted.reset_index() 
    df_all = pd.concat([df_current, previous_sar], keys=['Current_SAR', 'Previous_SAR'], axis =1)
    df_all = df_all.swaplevel(axis='columns')[df_current.columns[1:]]
    
    def highlight_diff(data, color='pink'):
        attr = 'background-color: {}'.format(color)
        other = data.xs('Current_SAR', axis='columns', level=-1)
        return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                        index=data.index, columns=data.columns)
    
    df_highlighted = df_all.style.apply(highlight_diff, axis=None)
    
    #Save to GCS
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Script_Semi_Annual_Report.xlsx") as writer:
        summary.to_excel(writer, sheet_name="Summary", index=True)
        df_pivoted.to_excel(writer, sheet_name="FY", index=True)
        df_current.to_excel(writer, sheet_name="Unpivoted_Current_Version", index=False)
        df_highlighted.to_excel(writer, sheet_name="highlighted")
        
    return df_current, df_pivoted, summary