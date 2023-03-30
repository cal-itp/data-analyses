import _utils
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_list/"

def clean_columns(df, strings_to_remove:list):
    """
    Remove certain strings from columns.
    
    Example: I want to remove 'tircp_' 
    before all the column names.
    """
    
    strings_to_remove = r"({})".format("|".join(strings_to_remove))
    
    df.columns = df.columns.str.replace(strings_to_remove, "", regex=True)
    
    return df 

def add_project_category(df, project_category: str):
    """
    Add project category column
    to dataframe.
    """
    df['project_category'] = project_category
    
    return df