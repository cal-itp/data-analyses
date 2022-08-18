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

def clean_data(df):
    
    # convert columns
    columns_to_int = ['a1_locode', 'a2_senatedistc', 'a2_senate_dist_b', 'a2_assem_dist_b','a2_assem_dist_c','a2_congress_dist_b','a2_congress_dist_c','a2_proj_lat','a2_proj_long',
                  'a2_senate_dist_b','a2_senatedistc','p_un_sig_inter_new_roundabout','a4_emp_based_pct','a4_le_methods','a4_srts_le','a1_locode','a2_senatedistc','a2_senate_dist_b']
    for col in columns_to_int:
        gdf[col] = gdf[col].apply(get_num)
    
    #add geometry
    gdf = (geography_utils.create_point_geometry(df, longitude_col = 'a2_proj_long', latitude_col = 'a2_proj_lat'))

    return gdf
