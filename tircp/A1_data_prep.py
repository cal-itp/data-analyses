import pandas as pd
from calitp import *
import shared_utils

#GCS File Path:
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/tircp/"
FILE_NAME = "TIRCP_July_8_2022.xlsx"

#Crosswalk
import A5_crosswalks as crosswalks

"""
Functions
"""
#Some PPNO numbers are 5+. Slice them down to <= 5.
def ppno_slice(df):
    df = df.assign(ppno = df['ppno'].str.slice(start=0, stop=5))
    return df 

"""
Import the Data
"""
#Project Sheet
def load_project(): 
    #Load in 
    df = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME}", sheet_name="Project Tracking"))
    #Clean PPNO, strip down to <5 characters
    df = ppno_slice(df)
    return df

#Allocation Agreement Sheet
def load_allocation(): 
    #Load in 
    df =  to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME}", sheet_name="Agreement Allocations"))
    #Clean PPNO, strip down to <5 characters
    df = ppno_slice(df)
    return df

#Previous SAR - this one is fake
def load_previous_sar():
    file_to_sar = "Fake_SAR_July_14.xlsx"
    df = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}{file_to_sar}", sheet_name = "dataframe (3)"))
    return df

"""
Clean Up the Data:
"""
#Clean up project sheet 
def clean_project():
    df = load_project()

    # Replace PPNO values manually that are NaN
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

    """
    Some grant recipients have multiple spellings of their name. 
    E.g. BART versus Bay Area Rapid Transit
    """
    df["grant_recipient"] = df["grant_recipient"].replace(
        crosswalks.grant_recipients_projects
    )

    # Fill in nulls based on data type
    df = df.fillna(df.dtypes.replace({"float64": 0.0, "object": "None", "int64": 0}))

    # Replace FY 21/22 with Cycle 4
    df["award_cycle"].replace({"FY 21/22": 4}, inplace=True)

    # Coerce cols that are supposed to be numeric
    df["other_funds_involved"] = df["other_funds_involved"].apply(
        pd.to_numeric, errors="coerce"
    )

    # Add prefix
    df = df.add_prefix("project_")
    return df


"""
Allocation Sheet
"""
# List for columns that should be date 
date_columns = [
    "allocation_date",
    "completion_date",
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

def clean_allocation():
    df = load_allocation()

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
    df["ppno"] = df["ppno"].replace(crosswalks.ppno_crosswalk_allocation)

    """
    Some rows are not completely filled: drop them based on whether or not some
    cols are populated.
    """
    df = df.dropna(subset=["award_year", "grant_recipient", "ppno"])

    """
    #Replace some string values that are in date columns
    """
    df["_3rd_party_award_date"] = df["_3rd_party_award_date"].replace(
        crosswalks.allocation_3rd_party_date
    )
    df["led"] = df["led"].replace(crosswalks.allocation_led)
    df["completion_date"] = df["completion_date"].replace(
        crosswalks.allocation_completion_date
    )

    # Correcting string to 0
    df["expended_amount"] = (
        df["expended_amount"].replace({"Deallocation": 0}).astype("int64")
    )

    # Fill in NA based on data type
    df = df.fillna(df.dtypes.replace({"float64": 0.0, "object": "None"}))

    # Coerce dates to datetime
    for c in date_columns:
        df[c] = df[c].apply(pd.to_datetime, errors="coerce")

    # Add prefix
    df = df.add_prefix("allocation_")

    return df