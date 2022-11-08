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
    app_data = pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/atp/cleaned_cycle5&6.xlsx',  index_col=[0])
    funded_data = read_SUR_funding_data()
    
    df = join_funding_and_app_data(funded_data,
                                   app_data, 
                                   awarded_col= ['awarded'],
                                   sort_values_cols = ['project_app_id','a2_proj_scope_summary', 'project_cycle', 'awarded'],
                                   subset_cols = ['project_app_id','a2_proj_scope_summary','project_cycle'])
    
    ## read in county names to change acronym names
    ## using place names
    county_place_names = (to_snakecase(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/2020-place-names-locode.xlsx', sheet_name=1)))
    
    county_place_names = county_place_names>>select(_.county_name, _.co__name_abbr_)
    county_place_names['co__name_abbr_'] = county_place_names['co__name_abbr_'].str.upper()
    ## create dictionary to map full county names
    county_map = dict(county_place_names[['co__name_abbr_', 'county_name']].values)
    
    #map and fillna values with full county names from original column
    df['a2_county_2'] = df.a2_county.map(county_map)
    df['a2_county_2'] = df['a2_county_2'].fillna(df['a2_county'])
    
    # drop original column and rename county column
    columns_to_drop = ['a2_county']
    df = df.drop(columns = columns_to_drop)
    df = df.rename(columns= {'a2_county_2':'a2_county'})
    
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
    
    df['a1_imp_agcy_city'] = df['a1_imp_agcy_city'].str.title()
    
    return df

'''
Map geometry cleaning
'''

def check_point_in_state(df,
                        state_col,
                        state_col_value):
    df[state_col] = df[state_col].fillna('None')
    def validate_point(df):
        if (df[state_col] == 'None'):
            return 'Point Not In State'
        
        elif (df[state_col] == state_col_value):
            return 'Point In State'
        
        return df
    
    df['point_check'] = df.apply(validate_point, axis = 1)
    
    return df



'''
Report Functions
'''

def reorder_namecol(df,
                    og_name_col,
                    new_name_col, 
                    split_on,
                   order_on):
    
    df_copy = df>>filter(_[og_name_col].str.contains(split_on))>>select(_[og_name_col])
    df_copy[['name_pt1', 'name_pt2']] = df_copy[og_name_col].str.split(split_on, 1, expand=True)
    
    def order_new_name(df):
        if order_on == "pt1_pt2":
            return (df['name_pt1'] + ' ' + df['name_pt2'])
        elif order_on == "pt2_pt1":
            return (df['name_pt2'] + ' ' + df['name_pt1'])
        else:
            return (df['name_pt1'] + ' ' + df['name_pt2'])
        
    df_copy[new_name_col] = df_copy.apply(order_new_name, axis = 1)
    
    new_name_mapping = (dict(df_copy[[og_name_col, new_name_col]].values))
    
    df[new_name_col] = df[og_name_col].map(new_name_mapping)

    df[new_name_col] = df[new_name_col].fillna(df[og_name_col])
    

    
    return df

