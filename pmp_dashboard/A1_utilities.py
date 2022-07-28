import geopandas as gpd
import numpy as np
import pandas as pd
from calitp import *
from shared_utils import utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/pmp_dashboard/"

'''
Crosswalks, Lists, Variables
'''
#Group PEC Class descriptions into divisions
div_crosswalks= {
            "State & Fed Mass Trans": "DRMT",
            "Statewide Planning": "DOTP",
            "Research": "DRISI",
            "PSR/PSSR Development": "DOTP",
            "Rail": "DRMT",
            "Planning Administration": "DOTP",
            "Regional Planning": "DOTP",
        }

#A list to hold clean dataframes
my_clean_dataframes = []

'''
Functions that can be used across sheets
'''

'''
For PSOE and TPSOE sheets, subset out OE adjacent columns
and PS adjacent columns into their own dataframe, before
concating them
'''
def subset_psoe_tpsoe(df, ps_list: list, oe_list: list):
    df_ps = df[ps_list]
    df_oe = df[oe_list]
    return df_ps, df_oe

'''
Cleaning the PSOE and TPSOE sheets
prior to concating them by stripping columns of their prefixes 
'''
def cleaning_psoe_tpsoe(df, ps_or_oe: str):
    # Fill in the column type for either PS: personal services
    # or OE: operating expense
    df["type"] = ps_or_oe

    # Strip away the prefixes from column names
    # https://stackoverflow.com/questions/54097284/removing-suffix-from-dataframe-column-names-python
    # Create suffix
    suffix = f"{ps_or_oe}_"
    df.columns = df.columns.str.replace(suffix, "", regex=True)

    # There is a enc_+_exp_projection for OE: try and except to rename to projection
    # To match PS
    try:
        df = df.rename(columns={"enc_+_exp_projection": "projection"})
    except:
        pass

    return df

'''
Function that loads & cleans in raw data
'''
def import_raw_data(file_name: str, name_of_sheet: str, appropriations_to_filter: list):

    """
    Name_of_sheet: name of Excel tab that contains data
    Appropriations_to_filter: certain appropriations are filtered out
    but this is can change based on what ppl want. 
    A list of what to filter allows for
    flexibility.  The cleaned data frames are then held into a list 
    called my_clean_dataframes
    """
    df = pd.read_excel(f"{GCS_FILE_PATH}{file_name}", sheet_name=name_of_sheet)
    
    # Get rid of the unnecessary header info
    # Stuff like "Enterprise Datalink Production download as of 05/23/2022"
    df = df.iloc[13:].reset_index(drop=True)

    # The first row contains column names
    df.columns = df.iloc[0]

    # Drop the first row as they are now column names
    df = df.drop(df.index[0]).reset_index(drop=True)

    # Drop rows with NA in the certain cols,
    # Since those are probably the grand totals tagged at the end of the Excel sheet
    df = df.dropna(subset=["PEC Class"])
    
    #Snakecase         
    df = to_snakecase(df) 
         
    # Rename columns to mimc dashboard
    df = df.rename(
        columns={
            "ps_alloc": "ps_allocation",
            "ps_exp": "ps_expenditure",
            "ps_bal": "ps_balance",
            "total_projected_%": "total_%_expended",
            "oe_alloc": "oe_allocation",
            "oe_enc": "oe_encumbrance",
            "oe_exp": "oe_expenditure",
            "appr": "appropriation",
            "total_expended___encumbrance": "total_expenditure",
            "oe_bal_excl_pre_enc": "oe_balance",
            "oe__enc_+_oe_exp_projection": "oe_enc_+_oe_exp_projection",
        }
    )

    # Certain appropriation(s) are filtered out:
    df = df[~df.appropriation.isin(appropriations_to_filter)]

    # Narrow down division names
    df["division"] = df["pec_class_description"].replace(div_crosswalks)

    # Adding dataframe to an empty list
    my_clean_dataframes.append(df)

    return df

'''
Funds by Division Sheet
'''
def create_fund_by_division(df):
    # Drop excluded cols
    excluded_cols = ["appr_catg", "act__hours", "py_pos_alloc"]
    df = ap11.drop(columns=excluded_cols)

    # Add a blank column for notes
    df["notes"] = np.nan

    return df

'''
TPSOE 
'''
# Monetary cols to coerce into floats
tpsoe_monetary_cols = [
        "allocation",
        "expenditure",
        "balance",
        "encumbrance",
        "projection",
        "year_end_expendded_pace",
        "%_expended",
    ]

#Rearranging the cols to be the right order
tpsoe_order_of_cols = [
        "pec_class",
        "division",
        "fund",
        "fund_description",
        "appropriation",
        "type",
        "allocation",
        "expenditure",
        "balance",
        "encumbrance",
        "projection",
        "year_end_expendded_pace",
        "%_expended",
    ]

# Cols for OE
tpsoe_oe_list = [
    "fund",
    "fund_description",
    "appropriation",
    "pec_class",
    "division",
    "oe_allocation",
    "oe_encumbrance",
    "oe_expenditure",
    "oe_balance",
    "oe_enc_+_oe_exp_projection",
]

# Cols: for PS
tpsoe_ps_list = [
    "fund",
    "fund_description",
    "appropriation",
    "pec_class",
    "division",
    "ps_allocation",
    "ps_expenditure",
    "ps_balance",
    "ps_projection",
    "year_end_expendded_pace",
    "ps_%_expended",
]

#Create the TPSOE sheet
def create_tpsoe(df, oe_list: list, ps_ist:list): 
    #Can just subset right here
    tpsoe_oe = cleaning_tpsoe(df[oe_list], "oe")
    tpsoe_ps = cleaning_tpsoe(df[ps_ist], "ps")
    
    #Stack them on top of each other, now that the 
    #columns are the same name
    c1 = pd.concat([tpsoe_ps, tpsoe_oe], sort=False)
    
    #Correct order of columns
    c1 = c1[tpsoe_order_of_cols]

    # Add a notes column
    c1["notes"] = np.nan
    
    #Force monetary cols into floats
    c1[tpsoe_monetary_cols] = c1[tpsoe_monetary_cols].astype("float64")

    return c1

'''
Timeline 
All the AP data stacked on top of each other
'''
#How to automate this...instead of having to type in a tuple
def create_timeline(keys_list: tuple):
    '''
    Keys reflect the accounting period the dataframe is taken.
    Grabbing my_clean_dataframes which holds all my clean & filtered dfs
    Stacking them all, adding a column called "source" which gives a source
    of where the row was taken from
    '''
    df = (
    pd.concat(my_clean_dataframes, keys=keys_list)
    .rename_axis(("source", "tmp"))
    .reset_index(level=0)
    .reset_index(drop=True))
    
    #Drop and rename columns
    df = (df
          .drop(columns=["ap",])
          .rename(columns={"source": "ap"})
         )
    return df

'''
PSOE Timeline
'''
psoe_ps_cols = [
    "appr_catg",
    "fund",
    "fund_description",
    "appropriation",
    "pec_class",
    "division",
    "ps_allocation",
    "ps_expenditure",
    "ps_balance",
    "ps_projection",
    "ps_%_expended",
    "ap",
    "pec_class_description",
]

psoe_os_cols = [
    "appr_catg",
    "fund",
    "fund_description",
    "appropriation",
    "pec_class",
    "division",
    "oe_allocation",
    "oe_encumbrance",
    "oe_expenditure",
    "oe_balance",
    "oe_enc_+_oe_exp_projection",
    "oe_%_expended",
    "ap",
    "pec_class_description",
]

psoe_right_col_order = [
        "appr_catg",
        "fund",
        "fund_description",
        "appropriation",
        "division",
        "pec_class",
        "pec_class_description",
        "allocation",
        "expense",
        "balance",
        "projection",
        "%_expended",
        "ap",
        "type",
        "encumbrance",
    ]
def create_psoe_timeline(df, ps_list: list, oe_list: list):
    #Subset the dataframe into OE and PS only. Apply cleaning function.
    psoe_oe = cleaning_psoe_tpsoe(df[oe_list], "oe")
    psoe_ps = cleaning_psoe_tpsoe(df[ps_list], "ps")
    
    #Stack the two dataframes on top of each other
    c1 = pd.concat([psoe_ps, psoe_oe], sort=False)
    
    #Rename & oeorder to mimic original PMP
    c1 = c1[psoe_right_col_order].rename(columns={"expenditure": "expense"})

    # Add a notes column
    c1["notes"] = np.nan

    return c1