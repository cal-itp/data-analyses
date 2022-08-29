'''
Script to Read, Clean and Export DLA ATP Data
'''

import numpy as np
import pandas as pd
from siuba import *

from shared_utils import geography_utils
from dla_utils import _dla_utils

from calitp import to_snakecase

def read_in_data():
    main_details = to_snakecase(pd.read_excel("gs://calitp-analytics-data/data-analyses/dla/atp/Main Details.xls"))
    project_details = to_snakecase(pd.read_excel("gs://calitp-analytics-data/data-analyses/dla/atp/Project Details.xls"))
    
    df = pd.merge(main_details, project_details, how="outer", on=["project_app_id", "project_cycle"], indicator='matches')
    columns_to_drop = ['a1_imp_agcy_contact','a1_imp_agcy_email','a1_imp_agcy_phone',
                      'a1_proj_partner_contact', 'a1_proj_partner_email', 'a1_proj_partner_phone']
    df = df.drop(columns = columns_to_drop)
    #inplace=True)
    return df

def get_num(x):
    try:
        return int(x)
    except Exception:
        try:
            return float(x)
        except Exception:
            return x  

## function to condense the columns for various districts that are manually entered in 3 boxes of the application
## function will return "Needs Manual Assistance" to identify the cases that do not fit in the model.
def format_districts(df, col_a, col_b, col_c, new_col):
    
    #rename columns to alias
    df = df.rename(columns = {col_a:'a',
                              col_b:'b',
                              col_c:'c'})
    #fix types
    df = df.astype({'a':'Int64',
                    'b':'Int64',
                    'c':'Int64'})
    
    #replace null values with numeric
    df["a"].fillna(9999999, inplace=True)
    df["b"].fillna(9999999, inplace=True)
    df["c"].fillna(9999999, inplace=True)
    
    def district_status(row):
        if (row.a == 0) and (row.b < 10) and (row.c < 10):
            return (str(row["b"])) + (str(row["c"]))
        
        elif (row.a < 10) and (row.b < 10) and not (row.c == 9999999):
            return (str(row["a"])) + (str(row["b"]))
        
        elif (row.a>=1) and (row.b == 9999999) and (row.c == 9999999):
            return (row["a"])
        
        elif (row.a >= 10) and (row.b>= 10) and (row.c == 9999999):
            return (str(row["a"])) + ', ' + (str(row["b"]))
        
        elif (row.a >= 10) and (row.b == 9999999)  and (row.c >= 10):
            return  (str(row["a"])) + ', ' + (str(row["c"]))
        
        elif (row.a >= 1) and (row.b == 0) and (row.c == 0):
            return  (str(row["a"])) 
        
        elif (row.a >= 1) and not (row.b == 0) and not (row.b == 9999999) and not (row.c == 9999999):
            return  (str(row["a"])) + ', ' + (str(row["b"])) + ', ' + (str(row["c"]))
        
        ## return string to identify which cases do not fit.
        else:
            return "Needs Manual Assistance"
    
    #apply function
    df[new_col] = df.apply(lambda x: district_status(x), axis=1)
    
    #replace values back to null
    df = df.replace({'a': 9999999, 'b': 9999999, 'c':9999999}, np.nan)
    
    #rename columns back to original
    df = df.rename(columns = {'a':col_a,
                              'b':col_b,
                              'c':col_c})
  
    return df

#change most of the columns with zeros to NaNs
## keeping some of the columns for check purposes and other analyses (ex. pct columns and counts). 
def convert_zeros_to_nan(df):
    df_zero = df.loc[:, df.eq(0).any()]
    df_zero.drop(['a2_assem_dist_b','a2_assem_dist_c', 'a2_congress_dist_b', 'a2_congress_dist_c', 'a2_senate_dist_b', 'a2_senatedistc',
              'a2_past_proj_qty', 'a3_st_num_schools', 'agency_app_num',
             'a3_st_ped_pct', 'a3_trail_trans_pct', 'a4_ped_gap_pct',  'a4_reg_init_pct', 'a4_com_init_pct',
              'a4_safe_route_pct', 'a4_fl_mile_pct', 'a4_emp_based_pct', 'a4_other_ni_pct'
             ], axis=1, inplace=True)
    df_zero_list = df_zero.columns.to_list()
    df[df_zero_list] = df[df_zero_list].replace({'0':np.nan, 0:np.nan})
    
    return df

def clean_data(df):
    
    # # convert columns
    # columns_to_int = ['a1_locode', 'a2_senatedistc', 'a2_senate_dist_b', 'a2_assem_dist_b','a2_assem_dist_c','a2_congress_dist_b','a2_congress_dist_c','a2_proj_lat','a2_proj_long',
    #               'a2_senate_dist_b','a2_senatedistc','p_un_sig_inter_new_roundabout','a4_emp_based_pct','a4_le_methods','a4_srts_le','a1_locode','a2_senatedistc','a2_senate_dist_b']
    # for col in columns_to_int:
    #     df[col] = df[col].apply(get_num)
        
    #condense assembly, congressional and senate district columns
    df = (format_districts(df, "a2_assem_dist_a", "a2_assem_dist_b", "a2_assem_dist_c", "assembly_district"))
    df = (format_districts(df, "a2_congress_dist_a", "a2_congress_dist_b", "a2_congress_dist_c", "congressional_district"))
    df = (format_districts(df, "a2_senate_dist_a", "a2_senate_dist_b", "a2_senatedistc", "senate_district"))
    
    #change most of the columns with zeros to NaNs
    ## keeping some of the columns for check purposes and other analyses (ex. pct columns and counts). 
    df = convert_zeros_to_nan(df)
    
    #add geometry
    gdf = (geography_utils.create_point_geometry(df, longitude_col = 'a2_proj_long', latitude_col = 'a2_proj_lat'))

    return gdf