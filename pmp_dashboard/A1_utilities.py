import numpy as np
import pandas as pd
from calitp import *

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/pmp_dashboard/"

"""
Crosswalks, Lists, Variables
"""
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



"""
Functions that can be used across sheets
"""
def cleaning_psoe_tpsoe(df, ps_or_oe: str):

    """ 
    Cleaning the PSOE and TPSOE sheets
    prior to concating them by stripping columns of their prefixes   
    """
    df["type"] = ps_or_oe

    """
    Strip away the prefixes from column names
    https://stackoverflow.com/questions/54097284/removing-suffix-from-dataframe-column-names-python
    """
    suffix = f"{ps_or_oe}_"
    df.columns = df.columns.str.replace(suffix, "", regex=True)

    return df

# Remove underscores, title case, and strip whitespaces
def clean_up_columns(df):
    df.columns = df.columns.str.replace("_", " ").str.title().str.strip()
    return df

"""
Function that loads & cleans raw data
"""
int_cols = [
    "ps_alloc",
    "ps_exp",
    "ps_bal",
    "py_pos_alloc",
    "act__hours",
    "oe_alloc",
    "oe_enc",
    "oe_exp",
    "oe_bal_excl_pre_enc",
]


def import_and_clean(
    file_name: str,
    name_of_sheet: str,
    appropriations_to_filter: list,
    accounting_period: int,
):

    """Load the raw data, clean it up, add all additional cols

    Args:
        file_name: the Excel workbook
        name_of_sheet: the name of the sheet
        appropriations_to_filter: list of all the appropriations to be filtered out
        ap: enter the accounting period this is

    Returns:
        The cleaned df. Input the results into my list called my_cleaned_dataframes.

    """
    df = pd.read_excel(f"{GCS_FILE_PATH}{file_name}", sheet_name=name_of_sheet)

    # Get rid of the unnecessary header info
    # Stuff like "Enterprise Datalink Production download as of 05/23/2022"
    df = df.iloc[13:].reset_index(drop=True)

    # The first row contains column names - update it to the column
    df.columns = df.iloc[0]

    # Drop the first row as they are now column names
    df = df.drop(df.index[0]).reset_index(drop=True)

    # Drop rows with NA in PEC Class
    # Since those are probably the grand totals tagged at the end of the Excel sheet
    df = df.dropna(subset=["PEC Class"])

    # Snakecase
    df = to_snakecase(df)

    # Certain appropriation(s) are filtered out:
    df = df[~df.appr.isin(appropriations_to_filter)]
    
    # Change to the right data type
    df[int_cols] = df[int_cols].astype("float64").fillna(0)
    """
    Create Columns
    """
    # Fill in a column with the accounting period
    df["ap"] = accounting_period

    # Create a variable that just captures one instance of the ap,
    # this is used in certain calculations for columns
    df = df.assign(
          ps_projection = (df["ps_exp"] / accounting_period) * 12,
          oe_enc_plus_oe_exp_projection = (df["oe_enc"] + df["oe_exp"] / accounting_period * 12).astype("float64"),
          total_expenditure = (df["oe_enc"] + df["oe_exp"] + df["ps_exp"]),
          total_allocation = df["oe_alloc"] + df["ps_alloc"],
          )
    df = df.assign(ps_percent_expended=(df["ps_exp"] / df["ps_alloc"]).fillna(0), 
          year_expended_pace = (df["ps_projection"] / df["ps_alloc"]).fillna(0),
          oe_percent_expended = (df["oe_enc_plus_oe_exp_projection"] / df["oe_alloc"]).fillna(0),
          division = df["pec_class_description"].replace(div_crosswalks),
          total_balance = df["ps_bal"] + df["oe_bal_excl_pre_enc"],
          total_projection = df["ps_projection"] + df["oe_enc_plus_oe_exp_projection"],
          total_percent_expended = (df["total_expenditure"] / df["total_allocation"]).fillna(0)
                  )

    
    # Rename columns to mimc dashboard
    df = df.rename(
        columns={
            "ps_alloc": "ps_allocation",
            "ps_exp": "ps_expenditure",
            "ps_bal": "ps_balance",
            "oe_alloc": "oe_allocation",
            "oe_enc": "oe_encumbrance",
            "oe_exp": "oe_expenditure",
            "appr": "appropriation",
            "oe_bal_excl_pre_enc": "oe_balance",
            "oe_enc_plus_oe_exp_projection":"oe_enc_+_oe_exp_projection",
            "oe_percent_expended": "oe_%_expended",
            "total_percent_expended": "total_%_expended",
            "ps_percent_expended": "ps_%_expended"
        }
    )
    
    return df

'''
Funds by Division Sheet
'''
div_funds_right_order = [
    "pec_class",
    "division",
    "fund",
    "fund_description",
    "appropriation",
    "ps_allocation",
    "ps_expenditure",
    "ps_balance",
    "ps_projection",
    "year_expended_pace",
    "ps_%_expended",
    "oe_allocation",
    "oe_encumbrance",
    "oe_expenditure",
    "oe_balance",
    "oe_enc_+_oe_exp_projection",
    "oe_%_expended",
    "total_allocation",
    "total_expenditure",
    "total_balance",
    "total_projection",
    "total_%_expended",
    "notes",
]

def create_fund_by_division(df):
    
    # Drop excluded cols
    excluded_cols = ["appr_catg", "act__hours", "py_pos_alloc", "pec_class_description", "ap"]
    df = df.drop(columns=excluded_cols)
    
    # Add a blank column for notes
    df["notes"] = np.nan
    
    # Rearrange the columns to the right order
    df = df[div_funds_right_order]
    
    # Clean up col names
    df = clean_up_columns(df)
    
    return df

'''
TPSOE Sheet
'''
# Columns relevant PS
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
    "year_expended_pace",
    "ps_%_expended",
]

# Columns relevant OE
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
    "oe_projection",
]

# Monetary columns
tpsoe_monetary_cols = [
    "allocation",
    "expenditure",
    "balance",
    "encumbrance",
    "projection",
]

# Ordering the columns correctly
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
    "year_expended_pace",
    "%_expended",
]

# Create the sheet
def create_tpsoe(df, ps_list: list, oe_list: list):
    """
    ps_list: a list of all the ps related columns.
    oe_list: a list of all the oe related columns.
    Use this to subset out the whole dataframe,
    one for personal services, one for operating expenses.
    """
    # Rename this column so I can concat both sheets properly
    df = df.rename(columns = {"oe_enc_+_oe_exp_projection": 
                              "oe_projection"}
                  ) 
    
    # Clean up and subset out the dataframe
    tpsoe_oe = cleaning_psoe_tpsoe(df[oe_list], "oe")
    tpsoe_ps = cleaning_psoe_tpsoe(df[ps_list], "ps")

    # Concat the two dataframes together
    c1 = pd.concat([tpsoe_ps, tpsoe_oe], sort=False)
    
    # Rearrange the columns to the right order
    c1 = c1[tpsoe_order_of_cols]
    
    # Correct data types of monetary columns from objects to float
    c1[tpsoe_monetary_cols] = c1[tpsoe_monetary_cols].astype("float64")
    
    # Reset index
    c1 = c1.reset_index(drop = True)
    
    # Fill in na
    c1 = c1.fillna(0)
    
    # Add a notes column
    c1["notes"] = np.nan
    
    # Clean up Columns
    c1 = clean_up_columns(c1)
    
    return c1

"""
Timeline Sheet
"""
timeline_right_order = [
    "appr_catg",
    "fund",
    "fund_description",
    "appropriation",
    "pec_class",
    "pec_class_description",
    "ps_allocation",
    "ps_expenditure",
    "ps_balance",
    "ps_%_expended",
    "ps_projection",
    "py_pos_alloc",
    "act__hours",
    "oe_allocation",
    "oe_encumbrance",
    "oe_expenditure",
    "oe_balance",
    "oe_enc_+_oe_exp_projection",
    "oe_%_expended",
    "total_allocation",
    "total_expenditure",
    "division",
    "total_balance",
    "total_projection",
    "total_%_expended",
    "ap",
]


def create_timeline(df):
    # Open up the sheet with all the accounting periods
    timeline_all_aps = pd.read_excel(
        f"{GCS_FILE_PATH}All_Accounting_Periods.xlsx", sheet_name="timeline"
    )

    # Drop irrelevant col(s)
    df = df.drop(columns="year_expended_pace")

    # Rearrange to the right order
    df = df[timeline_right_order]

    # Clean up col names
    df = clean_up_columns(df)

    # Concat current accounting period with previous data, reset index
    c1 = pd.concat([df, timeline_all_aps], sort=False).reset_index(drop = True)

    return c1

'''
PSOE Timeline
'''
# Columns relevant PS
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

# Columns relevant OE
psoe_oe_cols = [
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
    "oe_projection",
    "oe_%_expended",
    "ap",
    "pec_class_description",
]

# Reorder to the right order
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

    # Rename this column so I can concat both sheets properly
    df = df.rename(columns={"oe_enc_+_oe_exp_projection": "oe_projection"})

    # Create 2 dataframes that subsets out OE and PS
    psoe_oe = cleaning_psoe_tpsoe(df[oe_list], "oe")
    psoe_ps = cleaning_psoe_tpsoe(df[ps_list], "ps")

    # Stack both dataframes on top of each other
    c1 = pd.concat([psoe_ps, psoe_oe], sort=False)

    # Rename column
    c1 = c1.rename(columns={"expenditure": "expense"})

    # Rearrange the dataframe in the right order
    c1 = c1[psoe_right_col_order]

    # Fill in na
    c1 = c1.fillna(0)
    
    # Clean up col names
    c1 = clean_up_columns(c1)
    
    # Open up sheet with previous accounting periods
    psoe_all_aps = pd.read_excel(f"{GCS_FILE_PATH}All_Accounting_Periods.xlsx", sheet_name="psoe")

    # Concat current accounting period with previous data. Reset index.
    c2 = pd.concat([c1, psoe_all_aps], sort=False).reset_index(drop=True)

    return c2

"""
Final Script to 
bring everything together
"""
def pmp_dashboard_sheets(
    file_name: str,
    name_of_sheet: str,
    appropriations_to_filter: list,
    accounting_period: int,
    year: str,
):

    """Takes the original and returns
    the entire cleaned Excel workbook for publishing the PMP dashboard.

    Args:
        file_name: GCS path already defined, input file name
        name_of_sheet: name of Excel sheet.
        appropriations_to_filter: appropriations to delete varies in each month. 
        a list allows you to change
        accounting_period: the current data's accounting period, used to name
        the finished workbook and for certain column calculations
        year: the fiscal year used to name the finished workbook
    
    """
    # Running scripts for each sheet
    df = import_and_clean(
        file_name, name_of_sheet, appropriations_to_filter, accounting_period
    )
    fund_by_div = create_fund_by_division(df)
    tspoe = create_tpsoe(df, tpsoe_ps_list,tpsoe_oe_list)
    timeline = create_timeline(df)
    psoe = create_psoe_timeline(df, psoe_ps_cols,psoe_oe_cols)

    """
    # Filter out stuff for timeline
    unwanted = timeline[
        (timeline["Appropriation"] == unwanted_timeline_appropriations)
        & (timeline["Ps Allocation"] == 0)
        & (timeline["Oe Allocation"] == 0)
    ]
    timeline = timeline.drop(index=unwanted.index)
    timeline = timeline.reset_index(drop=True)
    """

    # Save to Excel workbook with data from every single accounting period
    with pd.ExcelWriter(f"{GCS_FILE_PATH}All_Accounting_Periods.xlsx") as writer:
        timeline.to_excel(writer, sheet_name="timeline", index=False)
        psoe.to_excel(writer, sheet_name="psoe", index=False)
 
    # Save this month's output
    with pd.ExcelWriter(f"{GCS_FILE_PATH}AP_{accounting_period}_{year}.xlsx") as writer:
        fund_by_div.to_excel(writer, sheet_name="fund_by_div", index=False)
        tspoe.to_excel(writer, sheet_name="tspoe", index=False)
        timeline.to_excel(writer, sheet_name="timeline", index=False)
        psoe.to_excel(writer, sheet_name="psoe", index=False)

if __name__ == '__main__': 
    FILE_NAME = "AP3 September.xls"
    NAME_OF_SHEET = "Download"
    APPROPRIATIONS_TO_FILTER = []
    ACCOUNTING_PERIOD =  3
    YEAR = "TEST_21_22"
    
    pmp_dashboard_sheets(FILE_NAME,NAME_OF_SHEET,APPROPRIATIONS_TO_FILTER,ACCOUNTING_PERIOD,YEAR)