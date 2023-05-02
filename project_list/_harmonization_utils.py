import _cleaning_utils
import _state_rail_plan_utils as srp_utils
import _sb1_utils as sb1_utils
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_list/"

"""
Load in cleaned up data
"""
def load_state_rail_plan():
    df = srp_utils.clean_state_rail_plan(srp_utils.state_rail_plan_file)
    return df

def load_lost():
    df = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}LOST/LOST_all_projects.xlsx", sheet_name = "Main"))
    return df

def load_sb1():
    return sb1_utils.sb1_final()

"""
Harmonizing
Functions
"""
def organization_cleaning(df, agency_col: str) -> pd.DataFrame:
    """
    Cleans up agency names. Assume anything after comma/()/
    ; are acronyms and delete them. Correct certain mispellings.
    Change agency names to title case. Clean whitespaces.
    """
    df[agency_col] = (
        df[agency_col]
        .str.strip()
        .str.split(",")
        .str[0]
        .str.replace("/", "")
        .str.split("(")
        .str[0]
        .str.split("/")
        .str[0]
        .str.split(";")
        .str[0]
        .str.title()
        .str.replace("Trasit", "Transit")
        .str.replace("*","")
        .str.strip() #strip whitespaces again after getting rid of certain things
    )
    return df

def funding_vs_expenses(df):
    """
    Determine if a project is fully funded or not
    """
    if df["total_project_cost"] == 0.00:
        return "No project cost info"
    elif df["total_available_funds"] == 0.00:
        return "No available funding info"
    elif (df["total_available_funds"] == df["total_project_cost"])|(df["total_available_funds"] > df["total_project_cost"]):
        return "Fully funded"
    else:
        return "Partially funded"

columns_to_keep = [
        "project_title",
        "lead_agency",
        "project_year",
        "project_category",
        "grant_program",
        "project_description",
        "total_project_cost",
        "fully_funded",
        "total_available_funds",
        "location",
         "city",
        "county",
        "data_source",
        "notes",
        "funding_notes",
        "project_id",
    ]

