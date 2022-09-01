"""
Tableau 
"""
import numpy as np
import pandas as pd
from calitp import *
import A5_crosswalks as crosswalks
import A1_data_prep

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/tircp/"

"""
Functions
"""
# Categorize a project by percentiles
def project_size_rating(dataframe, original_column: str, new_column: str):
    """Rate a project by percentiles and returning small/medium/large for any column
    
    Args:
        dataframe
        original_column (str): column to create the metric off of
        new_column (str): new column to hold results

    Returns:
        the dataframe with the new column with the categorization.

    """
    # Get percentiles in objects for total vehicle.
    p75 = dataframe[original_column].quantile(0.75).astype(float)
    p25 = dataframe[original_column].quantile(0.25).astype(float)
    p50 = dataframe[original_column].quantile(0.50).astype(float)

    # Function for fleet size
    def project_size(row):
        if (row[original_column] > 0) and (row[original_column] <= p25):
            return "Small"
        elif (row[original_column] > p25) and (row[original_column] <= p75):
            return "Medium"
        elif row[original_column] > p75:
            return "Large"
        else:
            return "No Info"

    dataframe[new_column] = dataframe.apply(lambda x: project_size(x), axis=1)

    return dataframe

# Categorizing expended percentage into bins
def expended_percent(row):
    if (row.Expended_Percent > 0) and (row.Expended_Percent < 0.26):
        return "1-25"
    elif (row.Expended_Percent > 0.25) and (row.Expended_Percent < 0.51):
        return "26-50"
    elif (row.Expended_Percent > 0.50) and (row.Expended_Percent < 0.76):
        return "51-75"
    elif (row.Expended_Percent > 0.75) and (row.Expended_Percent < 0.95):
        return "76-99"
    elif row.Expended_Percent == 0.0:
        return "0"
    else:
        return "100"


# Categorize years and expended_percent_group into bins
def progress(df):
    # 2015
    if (df["project_award_year"] == 2015) and (
        df["Expended_Percent_Group"] == "1-25"
    ) | (df["Expended_Percent_Group"] == "26-50"):
        return "Behind"
    elif (df["project_award_year"] == 2015) and (
        df["Expended_Percent_Group"] == "76-99"
    ) | (df["Expended_Percent_Group"] == "51-75"):
        return "On Track"

    # 2016
    elif (df["project_award_year"] == 2016) and (
        df["Expended_Percent_Group"] == "1-25"
    ) | (df["Expended_Percent_Group"] == "26-50"):
        return "Behind"
    elif (df["project_award_year"] == 2016) and (
        df["Expended_Percent_Group"] == "51-75"
    ) | (df["Expended_Percent_Group"] == "76-99"):
        return "On Track"

    # 2018
    elif (df["project_award_year"] == 2018) and (
        df["Expended_Percent_Group"] == "1-25"
    ):
        return "Behind"
    elif (df["project_award_year"] == 2018) and (
        df["Expended_Percent_Group"] == "26-50"
    ) | (df["Expended_Percent_Group"] == "51-75"):
        return "On Track"
    elif (df["project_award_year"] == 2018) and (
        df["Expended_Percent_Group"] == "76-99"
    ):
        return "Ahead"

    # 2020
    elif (df["project_award_year"] == 2020) and (
        df["Expended_Percent_Group"] == "1-25"
    ):
        return "Behind"
    elif (df["project_award_year"] == 2020) and (
        df["Expended_Percent_Group"] == "26-50"
    ):
        return "On Track"
    elif (df["project_award_year"] == 2020) and (
        df["Expended_Percent_Group"] == "51-75"
    ) | (df["Expended_Percent_Group"] == "76-99"):
        return "Ahead"

    # 0 Expenditures
    elif df["Expended_Percent_Group"] == "0":
        return "No expenditures recorded"

    else:
        return "100% of allocated funds spent"
"""
Lists
"""
burndown_cols_to_keep = [
    "allocation_ppno",
    "allocation_award_year",
    "project_project_title",
    "project_grant_recipient",
    "allocation_components",
    "project_total_project_cost",
    "project_tircp_award_amount__$_",
    "allocation_phase",
    "allocation_allocation_amount",
    "allocation_expended_amount",
    "allocation_completion_date",
]

"""
Script
"""
def tableau_dashboard():
    # Load in cleaned project sheets
    df = A1_data_prep.clean_project()

    # Replace districts & counties with their full names
    df["project_district"] = df["project_district"].replace(crosswalks.full_ct_district)
    df["project_county"] = df["project_county"].replace(crosswalks.full_county)

    # Create new cols
    df = df.assign(
        Expended_Percent=(
            df["project_expended_amount"] / df["project_allocated_amount"]
        ).fillna(0),
        Allocated_Percent=(
            df["project_allocated_amount"] / df["project_tircp_award_amount__$_"]
        ).fillna(0),
        Unallocated_Amount=(
            df["project_tircp_award_amount__$_"] - df["project_allocated_amount"]
        ).fillna(0),
        Projects_Funded_Percent=(
            df["project_tircp_award_amount__$_"] / df["project_total_project_cost"]
        ).fillna(0),
    )

    """
    Categorize projects whether they are ahead/behind/0 expenditures/etc
    """
    # Categorize projects first into expended % bins
    df["Expended_Percent_Group"] = df.apply(lambda x: expended_percent(x), axis=1)

    # Second, apply progress function
    df["Progress"] = df.apply(progress, axis=1)

    # Categorize projects whether they are large/small/med based on TIRCP amount
    df = df.rename(columns={"project_tircp_award_amount__$_": "tircp"})

    # Categorize projects by project size
    df = project_size_rating(df, "tircp", "Project_Category")

    # Clean Up Column Names
    df = A1_data_prep.clean_up_columns(df)

    return df

def create_burndown():
    project = A1_data_prep.clean_project()
    allocation = A1_data_prep.clean_allocation()

    # Merge the two sheets
    m1 = pd.merge(
        allocation,
        project,
        how="left",
        left_on=["allocation_ppno", "allocation_award_year"],
        right_on=["project_ppno", "project_award_year"],
        indicator=True,
    )

    # Keep only certain columns
    m1 = m1[burndown_cols_to_keep]

    # Clean up names
    m1 = A1_data_prep.clean_up_columns(m1)

    # Fill in null
    m1 = m1.fillna(m1.dtypes.replace({"float64": 0.0, "int64": 0, "object": "N/A"}))

    return m1

# Script to bring it together
def complete_tableau():
    burndown = create_burndown()
    tableau = tableau_dashboard()
    
    # Write to GCS
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Tableau_Workbook.xlsx") as writer:
        tableau.to_excel(writer, sheet_name="main", index=False)
        burndown.to_excel(writer, sheet_name = "burndown", index=False)
