import A1_data_prep
import pandas as pd
from calitp import *
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/tircp/"

"""
Variables
"""
missing_date = pd.to_datetime("2100-01-01")

"""
Columns
"""
# Columns to keep from project sheet
project_cols = [
    "project_award_year",
    "project_project_#",
    "project_tircp_award_amount__$_",
    "project_grant_recipient",
    "project_project_title",
    "project_ppno",
    "project_unallocated_amount",
]

# Columns to keep from allocation sheet
allocation_cols = [
    "allocation_award_year",
    "allocation_grant_recipient",
    "allocation_implementing_agency",
    "allocation_components",
    "allocation_ppno",
    "allocation_phase",
    "allocation_prior_fiscal_years_to_2020",
    "allocation_fiscal_year_2020_2021",
    "allocation_fiscal_year_2021_2022",
    "allocation_fiscal_year_2022_2023",
    "allocation_fiscal_year_2023_2024",
    "allocation_fiscal_year_2024_2025",
    "allocation_fiscal_year_2025_2026",
    "allocation_fiscal_year_2026_2027",
    "allocation_fiscal_year_2027_2028",
    "allocation_fiscal_year_2028_2029",
    "allocation_fiscal_year_2029_2030",
    "allocation_ctc_financial_resolution",
    "allocation_allocation_date",
    "allocation_project_id",
    "allocation_sb1_funding",
    "allocation_ggrf_funding",
    "allocation_allocation_amount",
]

group_by_cols = [
    "Project #",
    "Award No",
    "Award Amount",
    "Not Allocated",
    "Grant Recipient",
    "Implementing Agency",
    "Ppno",
    "Project Title",
    "Separable Phases/Components",
    "Phase",
    "Id",
    "Ctc Financial Resolution",
    "Allocation Date",
]

max_cols = [
    "Prior Fiscal Years To 2020",
    "Fiscal Year 2021 2022",
    "Fiscal Year 2022 2023",
    "Fiscal Year 2023 2024",
    "Fiscal Year 2024 2025",
    "Fiscal Year 2025 2026",
    "Fiscal Year 2026 2027",
    "Fiscal Year 2027 2028",
    "Fiscal Year 2028 2029",
    "Fiscal Year 2029 2030",
]

sum_cols = ["PTA-SB1_Amount", "Ggrf Funding", "Total Amount"]
monetary_cols = [max_cols + sum_cols] + ["Not Allocated", "Award Amount"]

def pivot(df):
    agg = df.groupby(group_by_cols).agg(
        {**{e: "max" for e in max_cols}, **{e: "sum" for e in sum_cols}}
    )
    return agg

"""
Program Allocation Plan
"""
def create_program_allocation_plan():

    # Load in Sheets
    df_project = A1_data_prep.clean_project()
    df_allocation = A1_data_prep.clean_allocation()

    # Only keeping certain columns
    df_project = df_project[project_cols]
    df_allocation = df_allocation[allocation_cols]

    # Merge
    m1 = df_allocation.merge(
        df_project,
        how="left",
        left_on=["allocation_ppno"],
        right_on=["project_ppno"],
    )

    # Clean Up
    # Delete one of the PPNO and Award Year Columns
    m1 = m1.drop(
        columns=[
            "allocation_award_year",
            "project_ppno",
            "allocation_grant_recipient",
        ]
    )

    # Fill in some columns with TBD so it'll show up
    m1[["allocation_project_id", "allocation_ctc_financial_resolution"]] = m1[
        ["allocation_project_id", "allocation_ctc_financial_resolution"]
    ].fillna(value="TBD")

    # Fill in missing dates with something random
    m1["allocation_allocation_date"] = m1["allocation_allocation_date"].fillna(
        missing_date
    )

    # Create Total_Amount Col
    m1["Total_Amount"] = m1["allocation_ggrf_funding"] + m1["allocation_sb1_funding"]

    # Create a column that concats award year + project #
    m1["Award No"] = (
        m1["project_award_year"].astype("str")
        + " : "
        + m1["project_project_#"].astype("str")
    )

    # Rename cols to the right names and take away snakecase
    m1 = A1_data_prep.clean_up_columns(m1)
    m1 = m1.rename(
        columns={
            "Tircp Award Amount  $": "Award Amount",
            "#": "Project #",
            "Ctc Financial_Resolution": "Allocation Resolution",
            "Sb1 Funding": "PTA-SB1_Amount",
            "Unallocated Amount": "Not Allocated",
            "Title": "Project Title",
            "Components": "Separable Phases/Components",
            "Date": "Allocation Date",
        }
    )

    # Format to currency
    for i in monetary_cols:
        m1 = A1_data_prep.currency_format(m1, i)

    # Create sheets
    df_2015 = pivot(m1.loc[m1["Award Year"] == 2015])
    df_2016 = pivot(m1.loc[m1["Award Year"] == 2016])
    df_2018 = pivot(m1.loc[m1["Award Year"] == 2018])
    df_2020 = pivot(m1.loc[m1["Award Year"] == 2020])
    df_2022 = pivot(m1.loc[m1["Award Year"] == 2022])
 
    # GCS
    with pd.ExcelWriter(f"{GCS_FILE_PATH}Script_Program_Allocation_Plan.xlsx") as writer:
        df_2015.to_excel(writer, sheet_name="2015_Cycle_1", index=True)
        df_2016.to_excel(writer, sheet_name="2016_Cycle_2", index=True)
        df_2018.to_excel(writer, sheet_name="2018_Cycle_3", index=True)
        df_2020.to_excel(writer, sheet_name="2020_Cycle_4", index=True)
        df_2022.to_excel(writer, sheet_name="2022_Cycle_5", index=True)
  
    return m1