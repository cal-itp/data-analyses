'''
Script to Read, Clean and Export DLA ATP Data
'''

import numpy as np
import pandas as pd
from siuba import *

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

