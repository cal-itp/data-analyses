import A1_data_prep
import numpy as np
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

# Clean up project IDS by deleting extra zeroes and making it 8 characters long
def clean_project_ids(df, project_id_col: str):
    # Cast to string
    df[project_id_col] = df[project_id_col].astype("str")

    # Remove extra zeroes that may appear in front of ID
    # https://stackoverflow.com/questions/45923189/remove-first-character-from-pandas-column-if-the-number-1
    df[project_id_col] = df[project_id_col].apply(
        lambda x: x[2:] if x.startswith("0") else x
    )

    # Slice to 8 digits if it exceeds that number
    df[project_id_col] = df[project_id_col].str.slice(start=0, stop=8)
    return df

"""
Allocation sheet of the TIRCP Tracking 2.0
Prepare & Clean
"""
# Narrow down only relevant columns
alloc_subset = [
        "allocation_project_#",
        "allocation_ppno",
        "allocation_components",
        "allocation_award_year",
        "allocation_ea",
        "allocation_project_id",
        "allocation_phase",
        "allocation_sb1_funding",
        "allocation_ggrf_funding",
    ]

# Groupby cols
group_by_cols = [
        "allocation_project_id",
        "allocation_ppno",
        "allocation_ea",
        "allocation_project_#",
        "allocation_components",
        "allocation_award_year",
        "allocation_phase",
    ]

   
def prep_allocation():
    
    df = A1_data_prep.clean_allocation()[alloc_subset]

    # Filter out project IDs that are none and projects starting in 2018
    df = (
        df[((df.allocation_project_id != "None") & (df.allocation_award_year >= 2018))]
    ).reset_index(drop=True)

    # Clean up Project IDs
    df = clean_project_ids(df, "allocation_project_id")

    # Group multiple project ID and phase into one row: sum up SB1 and GGRF
   
    df = (
        df.groupby(group_by_cols)
        .agg({"allocation_sb1_funding": "sum", "allocation_ggrf_funding": "sum"})
        .reset_index()
    )
    
    # Sum up SB1 and GGRF
    df["Sum of Allocation Amount"] = (
        df["allocation_sb1_funding"] + df["allocation_ggrf_funding"]
    )

    return df


def final_allocation():
    """
    After prepping allocation sheet, join it with projects tab of TIRCP Tracking 2.0
    to grab the project titles.
    """
    # Prepared allocation sheet
    alloc = prep_allocation()

    # Subsetted projects tab to get project title
    project = A1_data_prep.clean_project()[
        [
            "project_award_year",
            "project_project_#",
            "project_project_title",
            "project_ppno",
        ]
    ]

    # Filter project sheet for ones cycle 3+
    project = (project.loc[project.project_award_year > 2017]).reset_index(drop=True)

    # Merge allocation and project on PPNO
    m1 = pd.merge(
        alloc,
        project,
        how="left",
        left_on=["allocation_ppno", "allocation_project_#"],
        right_on=["project_ppno", "project_project_#"],
        indicator=True,
    )

    m1 = m1.drop(
        columns=[
            "project_award_year",
            "project_project_#",
            "project_ppno",
        ]
    )

    return m1

"""
Expenditures data from Data Link/AMS
Prepare & Clean
"""
def prep_expenditures():
    # Load in original sheet
    df = pd.read_excel(
        f"{A1_data_prep.GCS_FILE_PATH}3010_3020 Expenditure for fund 0046.xls",
        sheet_name="Download",
    )

    # First 10 columns or so are not data, drop them
    df = df.iloc[9:].reset_index(drop=True)

    # The first row contains column names - update it to be the column
    df.columns = df.iloc[0]

    # Drop the first row as they are now column names
    df = df.drop(df.index[0]).reset_index(drop=True)
    
    # Snakecase
    df = to_snakecase(df)

    # Only grab records from 2018 and beyond
    df = (df.loc[df["fy"] > "2017"]).reset_index()

    # Subset to only columns of interest
    subset = ["fy", "fund", "appr_unit", "project", "project_name", "tot_exp"]
    df = df[subset]

    # Clean up project ID
    df = clean_project_ids(df, "project")

    return df

# Filter out the appr_unit code that corresponds with either SB1/GGRF 
# (end ins 301R or 101) then aggregate the results by project ID and name.
# The same project ID can have mulitple rows, but we only want one row
# for one project ID.
def split_ggrf_sb1_expenditures(
    df,
    appr_unit_wanted: str,
):

    # Search through for the appr_unit
    df = df.loc[df.appr_unit.str.contains(appr_unit_wanted)].reset_index(drop=True)

    # Group by project ID
    df = (
        df.groupby(
            [
                "project",
            ]
        )
        .agg({"tot_exp": "sum"})
        .reset_index()
    )

    # Rename total expenditures column with the appr_unit
    df = df.rename(columns={"tot_exp": f"{appr_unit_wanted}_tot_exp"})

    return df

# The final dataframe for expenditures:
# split the data by GGRF and Sb1 so the df can go from long
# to wide
def final_expenditures():
    df = prep_expenditures()

    # Find and sum up GGRF funds
    df_ggrf = split_ggrf_sb1_expenditures(df, "301R")

    # Find and sum up SB1 funds
    df_sb1 = split_ggrf_sb1_expenditures(df, "101")

    m1 = pd.merge(
        df_ggrf,
        df_sb1,
        how="outer",
        on=["project"],
        indicator=True,
    )

    return m1

"""
Projects Status Data
Prepare & Clean
"""
def prep_project_status():
    df = pd.read_excel(
        f"{A1_data_prep.GCS_FILE_PATH}Project status for fund 0046 as of 12-5-22.xls",
        sheet_name="Download",
    )

    # First few rows are not data
    df = df.iloc[6:].reset_index(drop=True)

    # Cast first row as column names
    df.columns = df.iloc[0]

    # Drop the first row as they are now column names
    df = df.drop(df.index[0]).reset_index(drop=True)

    # Coerce monetary columns to numeric
    df[["Billed", "Reimbursements"]] = df[["Billed", "Reimbursements"]].apply(
        pd.to_numeric, errors="coerce"
    )
    # Group project id so the same ones will be on one line, instead of 
    # being split on two
    df = (
        df.groupby(["Project"])
        .agg({"Billed": "sum", "Reimbursements": "sum"})
        .reset_index()
    )

    df = to_snakecase(df)

    # Clean project names
    df = clean_project_ids(df, "project")

    return df

"""
First merge
merging allocation data with expenditures 
"""
def merge_allocation_expenditures():
    
    alloc = final_allocation()
    
    expenditure = final_expenditures()
    
    m1 = pd.merge(
        alloc.drop(columns=["_merge"]),
        expenditure.drop(columns=["_merge"]),
        how="left",
        left_on="allocation_project_id",
        right_on="project",
        indicator=True,
    )
    
    m1 = m1.rename(columns={"_merge": "Allocation_Expenditure_Merge"})
    
    m1.Allocation_Expenditure_Merge = m1.Allocation_Expenditure_Merge.replace(
    {
        "both": "Project ID in TIRCP Tracking and 3010_3020 Expenditure for fund 0046",
        "left_only": "Project ID only in TIRCP Tracking",
    })
    
    m1 = m1.drop(columns=["project"])
    
    m1 = m1.fillna(0)
    
    return m1

"""
Second merge: merging m1 above with the project_status data. 
"""
def merge2_project_status():
    
    m1 = merge_allocation_expenditures()
    
    project_status = prep_project_status()
    
    m2 = pd.merge(
    m1,
    project_status,
    how="left",
    left_on="allocation_project_id",
    right_on="project",
    indicator=True,)
    
    m2 = m2.rename(columns={"_merge": "Allocation_Project_Status_Merge"})
    
    m2.Allocation_Project_Status_Merge = m2.Allocation_Project_Status_Merge.replace(
    {
        "both": "Project ID in TIRCP Tracking and Project_Status",
        "left_only": "Project ID only in TIRCP Tracking",
    })
    
    m2 = m2.drop(columns=["project"])
    
    return m2

"""
Final Accounting Analysis
"""
# Tag projects with the appropriate comment
def comments(row):

    if (row["Allocation Expenditure Merge"] == "Project ID only in TIRCP Tracking") & (
        row["Allocation Project Status Merge"] == "Project ID only in TIRCP Tracking"
    ):
        return "Component Split: need to correct in AMS"
   
    elif row["Sum Of Allocation Amount"] == 0:
        return "Component Split: correct in AMS"
    
    elif row["Sum Of Allocation Amount"] == row["Sum of GGRF Funding"]:
        return "100% GGRF/0046R: no action needed"

    elif row["Sum Of Allocation Amount"] == row["Sum of Sb1 Funding"]:
        return "100% SB1/0046: no action needed"

    elif row["Remaining Allocation"] == 0:
        return "Fully expended: no action needed"
    
    elif row["Remaining Allocation"] < 0:
        return "Negative Remaining Allocation"

    elif row["Remaining Allocation"] >= 400000:
        return "More than $400k: need to correct funding"

    elif row["Remaining Allocation"] <= 400000:
        return "Less than $400k: no action needed"

    # Everything else is not enough info
    else:
        return "Manual Comment"

# Arrange columns to be the correct order for the final report before exporting
right_col_order = [
    "Award Year",
    "#",
    "Title",
    "Project ID",
    "Ea",
    "Components",
    "Phase",
    "Sum of Sb1 Funding",
    "Sum of GGRF Funding",
    "Sum Of Allocation Amount",
    "Sum of GGRF (0046, xx301R expenditure)",
    "Sum of SB1 (0046, xx101 Expenditure)",
    "Sum of Expenditure",
    "Sum Billed",
    "Sum of Reimbursements",
    "Remaining Allocation",
    "Allocation Expenditure Merge",
    "Allocation Project Status Merge",
]

rename_cols = {
        "Id": "Project ID",
        "Sb1 Funding": "Sum of Sb1 Funding",
        "Ggrf Funding": "Sum of GGRF Funding",
        "101 Tot Exp": "Sum of SB1 (0046, xx101 Expenditure)",
        "301R Tot Exp": "Sum of GGRF (0046, xx301R expenditure)",
        "Billed": "Sum Billed",
        "Reimbursements": "Sum of Reimbursements",
    }
# Using m2 produced from merge2_project_status(), create the 
# entire report plus a summary table by count of projects & 
# remaining allocations grouped by the "Comments" column.
def final_accounting_analysis():
    
    df = merge2_project_status()
    
    # Clean up columns
    df = A1_data_prep.clean_up_columns(df)
    
    df = df.rename(columns=rename_cols)
    
    # Create sum columns
    df["Sum of Expenditure"] = df["Sum of SB1 (0046, xx101 Expenditure)"] + df["Sum of GGRF (0046, xx301R expenditure)"]
    df["Remaining Allocation"] = df["Sum Of Allocation Amount"] - df["Sum of Expenditure"]
    
    # Rearrange columns to be the right order
    df = df[right_col_order]
    
    # Apply the comment function
    df["Comments"] = df.apply(comments, axis=1)
    
    # Fill more NA
    for i in ["Sum Billed", "Sum of Reimbursements"]:
        df[i] = df[i].fillna(0)
    
    # Aggregate
    agg1 = (df
            .groupby('Comments')
            .agg({'Project ID':'count','Remaining Allocation':'sum',})
            .reset_index()
            .sort_values('Project ID', ascending = False)
            .rename(columns = {'Project ID':'Total Projects'})
           )
    
    agg1['Remaining Allocation'] = agg1['Remaining Allocation'].map("${:,.2f}".format)
    
    # Export
    with pd.ExcelWriter(f"{A1_data_prep.GCS_FILE_PATH}accounting_analysis.xlsx") as writer:
        df.to_excel(writer, sheet_name="accounting_analysis", index=False)
        agg1.to_excel(writer, sheet_name="summary", index=True)
  
    return df,agg1