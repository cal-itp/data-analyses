import pandas as pd
from calitp import *

#GCS File Path:
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/tircp/"
FILE_NAME = "TIRCP_10_19_2022.xlsx"

#Crosswalk
import A5_crosswalks as crosswalks

"""
Functions
"""
# Some PPNO numbers are 5+. Slice them down to <= 5.
def ppno_slice(df):
    df = df.assign(ppno=df["ppno"].str.slice(start=0, stop=5))
    return df

# Some Project IDs numbers are 10+ & contains random characters
def project_id_slice(df):
    df["project_id"] = df["project_id"].str.replace("\n", "")
    df = df.assign(project_id=df["project_id"].str.slice(start=0, stop=10))
    return df

# Function to clean cols before exporting
def clean_up_columns(df):
    df.columns = (
        df.columns.str.replace("_", " ")
        .str.replace("project", "")
        .str.replace("allocation", "")
        .str.title()
        .str.strip()
    )
    return df

# Function to clean agency/organization names 
def organization_cleaning(df, column_wanted: str):
    df[column_wanted] = (
        df[column_wanted]
        .str.strip()
        .str.split(",")
        .str[0]
        .str.replace("/", "")
        .str.split("(")
        .str[0]
        .str.split("/")
        .str[0]
        .str.title()
        .str.replace("Trasit", "Transit")
        .str.strip() #strip again after getting rid of certain things
    )
    return df

# Format a column to a currency format
def currency_format(df, col_name: str):
    df[col_name] = "$" + (df[col_name].astype(float)).round(0).astype(str)
    return df

"""
Import the Data
"""
# Project Sheet
def load_project():
    # Load in
    df = to_snakecase(
        pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME}", sheet_name="Project Tracking")
    )
    # Clean PPNO, strip down to 4-5 characters
    df = ppno_slice(df)
    return df


# Allocation Agreement Sheet
def load_allocation():
    # Load in
    df = to_snakecase(
        pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME}", sheet_name="Agreement Allocations")
    )
    # Clean PPNO, all should be  4-5 characters
    df = ppno_slice(df)
    
    # Clean Project ID, all should be 10 characters
    df = project_id_slice(df)
    return df


# Previous SAR - this one is fake
def load_previous_sar():
    file_to_sar = "fake_sar.xlsx"
    df = to_snakecase(
        pd.read_excel(f"{GCS_FILE_PATH}{file_to_sar}", sheet_name="Unpivoted_Current_Version")
    )
    return df

# Invoice Sheet 
def load_invoice():

    # Load in
    df = to_snakecase(
        pd.read_excel(
            f"{GCS_FILE_PATH}{FILE_NAME}", sheet_name="Invoice Tracking Sheet"
        )
    )
    
    # Clean Project ID, all should be 10 characters
    df = project_id_slice(df)
    
    return df

"""
Clean Project Sheet 
"""
# Manual cleaning portion of project that will change with each sheet
def clean_project_manual(df):

    # Replace agencies with the right PPNO
    df.loc[
        (
            df["grant_recipient"]
            == "San Bernardino County Transportation Authority (SBCTA)"
        ),
        "ppno",
    ] = 1230
    df.loc[
        (df["grant_recipient"] == "Bay Area Rapid Transit District (BART)"), "ppno"
    ] = "CP060"
    df.loc[(df["grant_recipient"] == "Santa Monica Big Blue Bus"), "ppno"] = "CP071"
    df.loc[
        (df["grant_recipient"] == "Antelope Valley Transit Authority (AVTA)")
        & (df["award_year"] == 2020),
        "ppno",
    ] = "CP059"

    # Replace FY 21/22 with Cycle 4
    df["award_cycle"].replace({"FY 21/22": 4}, inplace=True)

    return df

# Clean up project sheet completely 
def clean_project():
    df = load_project()

    """
    Some grant recipients have multiple spellings of their name. 
    E.g. BART versus Bay Area Rapid Transit. Fix this.
    """
    df = organization_cleaning(df, "grant_recipient")
    df["grant_recipient"] = df["grant_recipient"].replace(
        crosswalks.grant_recipients_projects
    )

    # Fill in nulls based on data type
    df = df.fillna(df.dtypes.replace({"float64": 0.0, "object": "None", "int64": 0}))

    # Replace FY 21/22 with Cycle 4
    df["award_cycle"].replace({"FY 21/22": 4}, inplace=True)

    # Coerce cols that are supposed to be numeric
    df[["other_funds_involved","total_project_cost"]] = df[["other_funds_involved","total_project_cost"]].apply(
        pd.to_numeric, errors="coerce"
    )

    # As this is manual data, correct in a separate function
    df = clean_project_manual(df)

    # Add prefix
    df = df.add_prefix("project_")

    return df

"""
Allocation Sheet
"""
# List for columns that should be date 
date_columns = [
    "allocation_date",
    "phase_completion_date",
    "_3rd_party_award_date",
    "led",
    "date_regional_coordinator_receives_psa",
    "date_oc_receives_psa",
    "date_opm_receives_psa",
    "date_legal_receives_psa",
    "date_returned_to_pm",
    "date_psa_approved_by_local_agency",
    "date_signed_by_drmt",
    "psa_expiry_date",
    "date_branch_chief_receives_psa",
]

# Manual portion of cleaning the allocation
def clean_allocation_manual(df):
    
    # Replace some string values that are in date columns
    df["_3rd_party_award_date"] = df["_3rd_party_award_date"].replace(
        crosswalks.allocation_3rd_party_date
    )
    df["led"] = df["led"].replace(crosswalks.allocation_led)
    df["phase_completion_date"] = df["phase_completion_date"].replace(
        crosswalks.allocation_completion_date
    )

    # Replace PPNO using clean project as the source of truth
    df.loc[
        (
            df["grant_recipient"]
            == "San Bernardino County Transportation Authority (SBCTA)"
        )
        & (df["award_year"] == 2016),
        "ppno",
    ] = 1230

    # Some PPNO are NaN, sort by award year & grant recipient to backwards fill values
    df = df.sort_values(["award_year", "grant_recipient"])
    
    # Replace with PPNO with a crosswalk for everything else
    df["ppno"] = df["ppno"].replace(crosswalks.ppno_crosswalk_allocation)

    return df

# Clean the entire allocation sheet
def clean_allocation():
    df = load_allocation()

    """
    Some rows are not completely filled: drop them based on whether or not some
    cols are populated.
    """
    df = df.dropna(subset=["award_year", "grant_recipient", "ppno"])

    # Correcting string to 0
    df["expended_amount"] = (
        df["expended_amount"].replace({"Deallocation": 0}).astype("int64")
    )

    # Fill in NA based on data type
    df = df.fillna(df.dtypes.replace({"float64": 0.0, "object": "None"}))

    # Coerce dates to datetime
    for c in date_columns:
        df[c] = df[c].apply(pd.to_datetime, errors="coerce")

    # Clean organization name/de duplicate
    df = organization_cleaning(df, "grant_recipient")

    # Do some manual replacing of values
    df = clean_allocation_manual(df) 
    
    # Add prefix
    df = df.add_prefix("allocation_")

    return df

"""
Invoice Sheet
"""
invoice_date_cols = [
    "invoice_date",
    "date_invoice_received_by_drmt",
    "date_invoice_received_by_agpa",
    "date_invoice_approved_by_drmt",
    "date_submitted_to_accounting",
    "date_sco_paid",
]

invoice_monetary_cols = [
    "amount_sco_paid",
    "invoice_amount",
]

def clean_invoice():
    df = load_invoice()

    # Coerce dates to datetime
    for c in invoice_date_cols:
        df[c] = df[c].apply(pd.to_datetime, errors="coerce")

    # Coerce monetary cols to integer
    for c in invoice_monetary_cols:
        df[c] = df[c].apply(pd.to_numeric, errors="coerce")
 
    # Fill in NA based on data type
    df = df.fillna(
        df.dtypes.replace(
            {
                "float64": 0.0,
                "object": "None",
            }
        )
    )

    # Clean organization names and de deuplicate
    df = organization_cleaning(df, "implementing_agency")

    # Add prefix
    df = df.add_prefix("invoice_")
    
    return df