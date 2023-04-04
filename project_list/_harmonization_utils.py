import _utils
import _state_rail_plan_utils as srp_utils
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_list/"

"""
Load in data
"""
def load_state_rail_plan():
    df = srp_utils.clean_state_rail_plan(srp_utils.state_rail_plan_file)
    return df

def load_lost():
    df = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}LOST/LOST_all_projects.xlsx", sheet_name = "Main"))
    return df

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
        return "Not fully funded"

def harmonizing(df, 
                agency_name: str,
                project_name:str,
                project_description:str,
                project_category:str,
                project_cost:str,
                location:str,
                county:str,
                city:str,
                program:str,
                fund_cols:list,
                cost_in_millions:bool = True):
    """
    Take a dataset and change the column names/types to
    default names and formats.
    
    Add metric if the project is fully funded or not. 
    """
    # Rename columns
    rename_columns = {agency_name: 'lead_agency',
                      project_name: 'project_title',
                      project_description: 'project_description',
                      project_category:'project_category',
                      project_cost: 'total_project_cost',
                      location: 'location',
                      county: 'county',
                      city: 'city'}
    
    df = df.rename(columns = rename_columns)
    
    # Coerce cost/fund columns to right type
    cost_columns = df.columns[df.columns.str.contains("(cost|funds)")].tolist()
    for i in cost_columns:
        df[i]= df[i].apply(pd.to_numeric, errors = 'coerce').fillna(0)
    
    # Clean up string columns
    string_cols = df.select_dtypes(include=['object']).columns.to_list()
    for i in string_cols:
        df[i] = df[i].str.strip().str.title()
        
    # Clean agency names
    df = organization_cleaning(df, 'lead_agency')
    
    # Add data source
    df['data_source'] = program 
    
    # Divide cost columns by millions
    # If bool is set to True
    if cost_in_millions:
        for i in cost_columns:
            df[i] = df[i].divide(1_000_000)
    else:
        df
    
   # Create columns even if they don't exist, just to harmonize 
   # before concatting.
    if 'county' not in df:
        df['county'] = "None"
    if 'city' not in df:
        df['city'] = "None"
    if 'notes' not in df:
        df['notes'] = "None" 
    
    # Determine if the project completely funded or not?
    # Add up all available funds
    df['total_available_funds'] = df[fund_cols].sum(axis=1)
    
    # Compare if available funds is greater or equal to
    # total project cost
    df['fully_funded'] = df.apply(funding_vs_expenses, axis=1)
    
    # Only keep certain columns
    columns_to_keep = ['project_title','lead_agency','project_category','project_description',
                       'total_project_cost','fully_funded','total_available_funds',
                       'location','county','city','notes','data_source']
    df = df[columns_to_keep]
    
    # Fill in any nulls
    df = df.fillna(df.dtypes.replace({'float64': 0.0, 'object': 'None'}))

    return df

def add_all_projects():
    
    # Load original dataframes
    state_rail_plan = load_state_rail_plan()
    lost = load_lost()
    
    # Clean dataframes
    state_rail_plan = harmonizing(state_rail_plan, 'lead_agency', 'project_name','project_description','project_category','total_project_cost', 'corridor', '', '', 'State Rail Plan', []) 
    lost = harmonizing(lost, 'agency', 'project_title','project_description', 'project_category','cost__in_millions_', 'location', 'county','city', 'LOST', 
                       ['estimated_lost_funds','estimated_federal_funds', 'estimated_state_funds','estimated_local_funds', 'estimated_other_funds'], False) 
    
    # Concat
    all_projects = pd.concat([lost, state_rail_plan])
    
    return all_projects
