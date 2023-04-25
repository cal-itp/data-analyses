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

# ADD YEAR
def harmonizing(
    df,
    agency_name_col: str,
    project_name_col: str,
    project_description_col: str,
    project_category_col: str,
    project_cost_col: str,
    location_col: str,
    county_col: str,
    city_col: str,
    project_year_col: str,
    program_col: str,
    data_source: str,
    fund_cols: list,
    cost_in_millions: bool = True,
):
    """
    Take a dataset and change the column names/types to
    default names and formats.
    
    Add metric if the project is fully funded or not.
    """
    rename_columns = {
        agency_name_col: "lead_agency",
        project_name_col: "project_title",
        project_description_col: "project_description",
        project_category_col: "project_category",
        project_cost_col: "total_project_cost",
        location_col: "location",
        county_col: "county",
        city_col: "city",
        project_year_col: "project_year",
        program_col: "grant_program"
    }
    # Rename columns
    df = df.rename(columns=rename_columns)

    # Coerce cost/fund columns to right type
    cost_columns = df.columns[df.columns.str.contains("(cost|funds)")].tolist()
    for i in cost_columns:
        df[i] = df[i].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Clean up string columns
    string_cols = df.select_dtypes(include=["object"]).columns.to_list()
    for i in string_cols:
        df[i] = df[i].str.replace("_", " ").str.strip().str.title()
    
    # Clean agency names
    df = organization_cleaning(df, "lead_agency")

    # Add data source
    df["data_source"] = data_source

    # Divide cost columns by millions
    # If bool is set to True
    if cost_in_millions:
        for i in cost_columns:
            df[i] = df[i].divide(1_000_000)

    # Create columns even if they don't exist, just to harmonize
    # before concatting.
    create_columns = ["county","city","notes", "project_year", "project_category"]
    for column in create_columns:
        if column not in df:
            df[column] = "None"
    if "grant_program" not in df:
        df["grant_program"] = data_source

    # Determine if the project completely funded or not?
    # Add up all available funds
    df["total_available_funds"] = df[fund_cols].sum(axis=1)
    df["fully_funded"] = df.apply(harmonization_utils.funding_vs_expenses, axis=1)
    
    # Add new column with funding breakout 
    # Since it's summarized above and the details are suppressed.
    prefix = "_" 
    for column in fund_cols:
        df[f"{prefix}{column}"] = df[column].astype(str)
    str_fund_cols = [prefix + sub for sub in fund_cols]
    # https://stackoverflow.com/questions/65532480/how-to-combine-column-names-and-values
    def combine_funding(x):
        return ', '.join([col + ': ' + x[col] for col in str_fund_cols])
    df['funding_notes'] = df.apply(combine_funding, axis = 1)
    df['funding_notes'] = df['funding_notes'].str.replace('_',' ')
    
    # Create unique project id - first LOST project is LOST-1, 
    # second LOST is LOST-2, LOST-3, LOST-4, etc 
    df['project_id'] =  df.data_source + '-' + df.groupby('data_source').cumcount().astype('str')
    
    # Only keep certain columns
    df = df[columns_to_keep]

    # Fill in any nulls
    df = df.fillna(df.dtypes.replace({"float64": 0.0, "object": "None"}))

    return df
