'''
Script to Read, Clean and Export DLA ATP Data
'''

import numpy as np
import pandas as pd
from siuba import *

from shared_utils import geography_utils
from dla_utils import _dla_utils

from calitp import to_snakecase

GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/dla/atp/'

locodes = to_snakecase(pd.read_excel("gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/locodes_updated7122021.xlsx"))

#function to read in data
def read_in_data():
    main_details = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}Main Details.xls"))
    project_details = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}Project Details.xls"))
    
    df = pd.merge(main_details, project_details, how="outer", on=["project_app_id", "project_cycle"], indicator='matches')
    columns_to_drop = ['a1_imp_agcy_contact','a1_imp_agcy_email','a1_imp_agcy_phone',
                      'a1_proj_partner_contact', 'a1_proj_partner_email', 'a1_proj_partner_phone']
    df = df.drop(columns = columns_to_drop)
    #inplace=True)
    return df
  

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

def export_district_need_assistance(df):
    #export the issues to folder for now so that we can know which entries to check
    ## get the rows that need assistance
    assem_manual = df>>filter(_.assembly_district=="Needs Manual Assistance")
    congr_manual =  df>>filter(_.congressional_district=="Needs Manual Assistance")
    senate_manual =  df>>filter(_.senate_district=="Needs Manual Assistance")
    
    ## concat them into one df 
    needs_assistance = pd.concat([senate_manual, congr_manual], ignore_index=True)
    needs_assistance = pd.concat([needs_assistance, assem_manual], ignore_index=True)
    
    ## drop duplicat entries
    needs_assistance = needs_assistance.drop_duplicates()
    
    needs_assistance = (needs_assistance>>select(_.a1_imp_agcy_city,  _.a1_imp_agcy_name, _.a1_imp_agcy_zip,
                                                 _.a1_imp_agcy_fed_ma_num, _.a1_imp_agcy_state_ma_num,
                                                 _.a2_assem_dist_a, _.a2_assem_dist_b, _.a2_assem_dist_c,
                                                 _.a2_congress_dist_a, _.a2_congress_dist_b, _.a2_congress_dist_c, 
                                                 _.a2_senate_dist_a, _.a2_senate_dist_b, _.a2_senatedistc,
                                                 _.assembly_district, _.congressional_district, _.senate_district))
    
    ## export to internal GCS bucket (can change)
    needs_assistance.to_excel(f"{GCS_FILE_PATH}needs_assistance/needs_assistance_districts.xlsx")
    
    
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

#function for getting int
def get_num(x):
    try:
        return int(x)
    except Exception:
        try:
            return float(x)
        except Exception:
            return x  

#dunction for checking the counties that agencies are in      
def check_counties(df):
    
    #subsetting df to just city, countyx, name and locode
    check_locode = df>>select(_.a1_imp_agcy_city, _.a2_county, _.a1_imp_agcy_name, _.a1_locode)
    check_locode['a1_locode'] = check_locode['a1_locode'].apply(get_num)
    
    #merge official locodes and subset
    (check_locode.merge(locodes, left_on='a1_imp_agcy_name', right_on='agency_name', how = 'outer', indicator = True))
    
    #look at those that just matched
    both = (check_locode.merge(locodes, left_on='a1_locode', right_on='agency_locode', how = 'outer', indicator = True))>>filter(_._merge == 'both')
    both['agency_locode'] = both['agency_locode'].astype('int')
    
    #add county to county names
    both['a2_county'] = both['a2_county'].astype(str) + ' County'  
    
    #add compare column to compare county names
    compare_names = np.where(both["a2_county"] == both["county_name"], True, False)
    both["compare_county"] = compare_names
    
    #get those where compare_county is false
    no_county_match = (both>>filter(_.compare_county==False))

    no_county_match = no_county_match.drop(columns = ['_merge','active_e76s______7_12_2021_',
                                                      'rtpa_name','mpo_name','mpo_locode_fads'])
    
    no_county_match = no_county_match.rename(columns={'compare_county':'county_match_on_name',
                                                         'agency_locode':'locode_list_agency_locode',
                                                         'agency_name':'locode_list_agency_name',
                                                         'district':'locode_list_district',
                                                         'county_name':'locode_list_county_name'})
    #export failed matched to gcs
    no_county_match.to_excel(f"{GCS_FILE_PATH}needs_assistance/failed_locode_county_check.xlsx")


# function to find potential locode matches
def find_potential_locode_matches(df):
        
    check_locode = df>>select(_.a1_imp_agcy_city, _.a2_county, _.a1_imp_agcy_name, _.a1_locode)
    check_locode['a1_locode'] = check_locode['a1_locode'].apply(get_num)

    remaining_locodes = ((check_locode.merge(locodes, left_on='a1_locode', right_on='agency_locode', how = 'outer', indicator = True))
    >>filter(_._merge== 'left_only'))
    
    remaining_locodes = remaining_locodes.drop(columns=['agency_name',
                                                        'district', 'county_name',
                                                        'rtpa_name', 'mpo_name',
                                                        'mpo_locode_fads', 'active_e76s______7_12_2021_'])
    
    #convert locodes to list of names
    names_list = locodes['agency_name'].tolist()
    pattern_names = '|'.join(names_list)
    
    #using str contains to see if there are any potnential matches with the official dla locodes
    remaining_locodes['contains'] = remaining_locodes['a1_imp_agcy_name'].str.contains(pattern_names, case=False)
    
    #get list of those with no matches
    no_match = remaining_locodes >>filter(_.contains==False)
    no_match.to_excel(f"{GCS_FILE_PATH}needs_assistance/no_match_locodes.xlsx")
    
    #get list of those with potential matches
    matches = (remaining_locodes>>filter(_.contains==True))
    matches['join'] = 1
    locodes['join'] = 1

    df_full = matches.merge(locodes, on='join').drop('join', axis=1)
    
    locodes.drop('join', axis=1, inplace=True)
    
    df_full['match'] = df_full.apply(lambda x: x.agency_name.find(x.a1_imp_agcy_city), axis=1).ge(0)    
    
    potential_matches = (df_full>>filter(_.match==True))
    potential_matches = potential_matches.drop(columns = ['agency_locode_x', '_merge',
                                                          'active_e76s______7_12_2021_',
                                                          'rtpa_name','mpo_name',
                                                          'mpo_locode_fads', 'match'])
    
    potential_matches = potential_matches.rename(columns={'contains':'potential_match',
                                                         'agency_locode_y':'locode_list_agency_locode',
                                                         'agency_name':'locode_list_agency_name',
                                                         'district':'locode_list_district',
                                                         'county_name':'locode_list_county_name'})
    
    # export potential match list
    potential_matches.to_excel(f"{GCS_FILE_PATH}needs_assistance/needs_assistance_potential_match_locodes.xlsx")


def clean_data(df):
        
    #condense assembly, congressional and senate district columns
    df = (format_districts(df, "a2_assem_dist_a", "a2_assem_dist_b", "a2_assem_dist_c", "assembly_district"))
    df = (format_districts(df, "a2_congress_dist_a", "a2_congress_dist_b", "a2_congress_dist_c", "congressional_district"))
    df = (format_districts(df, "a2_senate_dist_a", "a2_senate_dist_b", "a2_senatedistc", "senate_district"))
    
    #export the issues to folder for now so that we can know which entries to check
    ## export to internal GCS bucket (can change)
    export_district_need_assistance(df)
    
    #change most of the columns with zeros to NaNs
    ## keeping some of the columns for check purposes and other analyses (ex. pct columns and counts). 
    df = convert_zeros_to_nan(df)
    
    #convert df columns locode to Int64. Change 'None' values to nulls.
    df = df.replace({'a1_locode': 'None'}, np.nan)
    df = df.astype({'a1_locode':'Int64'})
    
    #check locodes
    locodes = to_snakecase(pd.read_excel("gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/locodes_updated7122021.xlsx"))
    
    check_counties(df) 
    find_potential_locode_matches(df)
    
    #add columns in cleaned df that were not in project details or main detail sheets.
    df["#"] = ""
    df["atp_id"] = ""
    df["awarded"] = ""
    df["ppno"] = ""
    df["ppno_1"] = ""
    
    df = df.drop(columns=['matches'])
    
    #add geometry for lat long column
    gdf = (geography_utils.create_point_geometry(df, longitude_col = 'a2_proj_long', latitude_col = 'a2_proj_lat'))

    return gdf