import _utils
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_list/"
state_rail_plan_file = "State Rail Plan/230307_CapitalProjectReview.xlsx"

def open_state_rail_plan(file_name):
    
    sheet_names = ["SRP-Appx3.1-Capital", "SRP-Appx3.2-Fleet", "SRP-Appx3.3-GradeSep"]
    
    # Open the workbook in a dictionary
    dict_df = pd.read_excel(f"{_utils.GCS_FILE_PATH}{file_name}", sheet_name=sheet_names)
    
    # Grab each sheet
    capital_df = to_snakecase(dict_df.get("SRP-Appx3.1-Capital"))
    fleet_df = to_snakecase(dict_df.get("SRP-Appx3.2-Fleet"))
    grade_sep_df = to_snakecase(dict_df.get("SRP-Appx3.3-GradeSep"))
    
    return capital_df, fleet_df, grade_sep_df

def clean_state_rail_plan(file_name):
    
    capital_df, fleet_df, grade_sep_df = open_state_rail_plan(file_name)
    
    column_strings_to_del = ['capital_','fleet_','grade_separation_']
        
    # Clean up columns
    capital_df = _utils.clean_columns(capital_df, column_strings_to_del)
    fleet_df = _utils.clean_columns(fleet_df, column_strings_to_del)
    grade_sep_df = _utils.clean_columns(grade_sep_df, column_strings_to_del)
    
    # Add project categories
    capital_df['project_category'] = "Capital"
    fleet_df['project_category'] = "Fleet"
    grade_sep_df['project_category'] = "Grade Separation"
    
    # Concat
    state_rail_plan_df = pd.concat([capital_df, fleet_df,grade_sep_df])

    return state_rail_plan_df
    