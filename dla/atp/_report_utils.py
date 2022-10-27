'''
Utils for Report Data Prep and Reports:

Functions combine the funded and application data to get one row for each project. 
From there was can use the functions for the report
'''

import intake
import numpy as np
import pandas as pd
from calitp import to_snakecase
from dla_utils import _dla_utils
from IPython.display import HTML, Markdown
from siuba import *

import altair as alt

import _data_cleaning

#import data_cleaning


'''
Read Data
'''

GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/dla/atp/'


## function reads in funding data (projects identified to get funding with funding amounts)
def read_SUR_funding_data():
    """
    Function to read in ATP funding data. Function will need to change for future data.
    Notes:
    * `atp_id` columns appear the same but the sur_details has an extra zero in the middle of the string so it would not match
    * `a3_project_type` also is entered differently however, details has more details than the funding sheet. Has information on size of project. can add to new column
    * `a1_imp_agcy_name_x` has manual errors so selecting `a1_imp_agcy_name_y`
    """
    # identify information columns that we need to drop
    columns_to_drop = ['a1_imp_agcy_contact','a1_imp_agcy_email','a1_imp_agcy_phone',
                      'a1_proj_partner_contact', 'a1_proj_partner_email', 'a1_proj_partner_phone']
    #read in SUR details and SUR funding data
    sur_details = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}Master_AllData_Cycle5_Field_Mapping_COPY.xls',
              sheet_name='Statewide SUR Details'))
    sur_details = sur_details.drop(columns = columns_to_drop)
    
    sur_funding = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}Master_AllData_Cycle5_Field_Mapping_COPY.xls',
              sheet_name='Statewide SUR Funding'))
    
    #drop the last few columns of SUR Details that have no funding data entered, but have columns
    sur_details.drop(sur_details.iloc[:,199:], inplace=True, axis=1)
    
    #remove rows with all null values
    cols_to_check = sur_funding.columns
    sur_funding['is_na'] = sur_funding[cols_to_check].isnull().apply(lambda x: all(x), axis=1) 
    sur_funding = sur_funding>>filter(_.is_na==False)
    sur_funding = sur_funding.drop(columns={'is_na'})

    #delete rows identified that are not part of the data (informational cells) or a sum total for all entries
    delete_row = sur_funding[sur_funding["project_cycle"]== 'Added Field not from App'].index
    sur_funding = sur_funding.drop(delete_row)
    
    delete_row = sur_funding[sur_funding["total_project_cost"]== '370,984,000.00'].index
    sur_funding = sur_funding.drop(delete_row)
    
    #merge sur_funding and sur_details
    merge_on = ['project_app_id', 'project_cycle', 'a2_ct_dist', 'a1_locode']
    df = (pd.merge(sur_details, sur_funding, how="outer", on = merge_on, indicator=True))
    
    #keep entries that merge. Right_only rows are misentered and more informational columns  
    df = df>>filter(_._merge=='both')
    
    # filling the null values for some of the duplicate columns
    # manually checking that values are the same as of now- will add function to check when we get the data links
    df['awarded_x'] = df['awarded_x'].fillna(df['awarded_y'])
    df['ppno_y'] = df['ppno_y'].fillna(df['ppno_x'])
    
    #renaming and dropping duplicate columns 
    ## a1_imp_agcy_name_x has manual errors so selecting a1_imp_agcy_name_y
    df = df.rename(columns={'awarded_x':'awarded',
                                'ppno_y':'ppno',
                                'a1_imp_agcy_name_y':'a1_imp_agcy_name',
                                'a2_info_proj_name_y':'a2_info_proj_name'
                               })
    df = df.drop(columns={'awarded_y', 'a1_imp_agcy_name_x', 'a2_info_proj_name_x','ppno_x', '_merge'})
    df["data_origin"]="Funded"
    
    return df

## put together funding data and application data
def join_funding_and_app_data(df_funding,
                              df_app,
                              awarded_col: list = [],
                             sort_values_cols: list = [],
                             subset_cols: list = []
                             ):
    '''
    columns in the funded and application data that we want to use
    awarded_col= ['awarded'],
    sort_values_cols = ['project_app_id','a2_proj_scope_summary', 'project_cycle', 'awarded'],
    subset_cols = ['project_app_id','a2_proj_scope_summary','project_cycle']
    '''
    
    # concat the funding and app dataframes
    df = (pd.concat([df_app, df_funding]))
    
    # take the awarded column and convert to a category so we can order by this column
    df[awarded_col] = df[awarded_col].astype('category') 
  #  df[awarded_col] = df[awarded_col].cat.set_categories(['Y', 'N'], ordered=True) 
    
    # sort values based on columns we defined (usually key like unique id, cycle)
    df.sort_values(sort_values_cols, inplace=True, ascending=True) 
    
    # drop duplicates so we only get the funded data instead of the application data for a project that is selected
    df_final = df.sort_values(awarded_col).drop_duplicates(subset=subset_cols, keep='last')
    
    return df_final
    

## read in the joined data so we only have to use one function
def read_in_joined_data():
    app_data = _data_cleaning.read_clean_data()
    funded_data = read_SUR_funding_data()
    
    df = join_funding_and_app_data(funded_data,
                                   app_data, 
                                   awarded_col= ['awarded'],
                                   sort_values_cols = ['project_app_id','a2_proj_scope_summary', 'project_cycle', 'awarded'],
                                   subset_cols = ['project_app_id','a2_proj_scope_summary','project_cycle'])
    
    
    ## Reorder cols to get app id in the front of the data frame
    ## https://stackoverflow.com/questions/41968732/set-order-of-columns-in-pandas-dataframe
    cols_to_order = [ 'project_app_id', 'project_cycle', 'a1_locode',
                 '#', 'atp_id', 'awarded', 'ppno', 'ppno_1',
                 'data_origin', 'geometry','project_status',
                 'solicitation_abv', 'solicitation', 'soliciting_agency', 'project_size',
                 'a1_imp_agcy_city', 'a1_imp_agcy_name', 
                 'a1_proj_partner_agcy', 'a1_proj_partner_exists',
                 'assembly_district', 'congressional_district', 'senate_district', 'a2_county', 'a2_ct_dist', 
                 'a2_info_proj_descr', 'a2_info_proj_loc', 'a2_info_proj_name', 'a2_mop_uza_population',
                 'a2_mpo', 'a2_rtpa',  'a2_proj_scope_summary']
        
    new_columns = cols_to_order + (df.columns.drop(cols_to_order).tolist())
    df = df[new_columns]
    
    return df


'''
Report Functions
'''



