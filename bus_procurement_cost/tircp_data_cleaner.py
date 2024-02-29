import numpy as np
import pandas as pd
import shared_utils
from calitp_data_analysis.sql import to_snakecase
from fta_data_cleaner import gcs_path
from dgs_data_cleaner import new_prop_finder
from dgs_data_cleaner import new_bus_size_finder

def col_row_updater(df: pd.DataFrame, col1: str, val1, col2: str, new_val):
    """
    function used to update values at specificed columns and row value.
    """
    df.loc[df[col1] == val1, col2] = new_val
    
    return

def clean_tircp_columns() -> pd.DataFrame:
    """
    main function that reads in and cleans TIRCP data.
    """
    file_name = "TIRCP Tracking Sheets 2_1-10-2024.xlsx"
    tircp_name = "Project Tracking"

    # read in data
    df = pd.read_excel(f"{gcs_path}{file_name}", sheet_name=tircp_name)

    # keep specific columns
    keep_col = [
        "Award Year",
        "Project #",
        "Grant Recipient",
        "Project Title",
        "PPNO",
        "District",
        "County",
        "Project Description",
        "bus_count",
        "Master Agreement Number",
        "Total Project Cost",
        "TIRCP Award Amount ($)",
    ]

    df1 = df[keep_col]

    # snakecase
    df2 = to_snakecase(df1)

    # dict of replacement values
    value_replace_dict = {
        "Antelope Valley Transit Authority ": "Antelope Valley Transit Authority (AVTA)",
        "Humboldt Transit Authority": "Humboldt Transit Authority (HTA)",
        "Orange County Transportation Authority": "Orange County Transportation Authority (OCTA)",
        "Capitol Corridor Joint Powers Authority": "Capitol Corridor Joint Powers Authority (CCJPA)",
        "Los Angeles County Metropolitan Transportation Authority": "Los Angeles County Metropolitan Transportation Authority (LA Metro)",
        "Monterey-Salinas Transit": "Monterey-Salinas Transit District (MST)",
        "Sacramento Regional Transit (SacRT)": "Sacramento Regional Transit District (SacRT)",
        "Sacramento Regional Transit District": "Sacramento Regional Transit District (SacRT)",
        "Sacramento Regional Transit District (SacRT) ": "Sacramento Regional Transit District (SacRT)",
        "San Diego Association of Governments": "San Diego Association of Governments (SANDAG)",
        "Santa Clara Valley Transportation Authority (SCVTA)": "Santa Clara Valley Transportation Authority (VTA)",
        "Southern California  Regional Rail Authority (SCRRA)": "Southern California Regional Rail Authority (SCRRA - Metrolink)",
        "Southern California Regional Rail Authority": "Southern California Regional Rail Authority (SCRRA - Metrolink)",
        "3, 4": "VAR",
    }
    
    # replacing values in agency & county col
    df3 = df2.replace(
        {"grant_recipient": value_replace_dict}
    ).replace(
        {"county": value_replace_dict}
    )
    
    # using update function to update values at specific columns and rows
    col_row_updater(df3, 'ppno', 'CP106', 'bus_count', 42)
    col_row_updater(df3, 'ppno', 'CP005', 'bus_count', 29)
    col_row_updater(df3, 'ppno', 'CP028', 'bus_count', 12)
    col_row_updater(df3, 'ppno', 'CP048', 'bus_count', 5)
    col_row_updater(df3, 'ppno', 'CP096', 'bus_count', 6)
    col_row_updater(df3, 'ppno', 'CP111', 'bus_count', 5)
    col_row_updater(df3, 'ppno', 'CP130', 'bus_count', 7)
    col_row_updater(df3, 'total_project_cost', 203651000, 'bus_count', 8)
    
    # columns to change dtype to str
    dtype_update = [
        'ppno',
        'district'
    ]
    
    df3[dtype_update] = df3[dtype_update].astype('str')
    
    # assigning new columns using imported functions.
    df4 = df3.assign(
        prop_type = df3['project_description'].apply(new_prop_finder),
        bus_size_type = df3['project_description'].apply(new_bus_size_finder)
    )

    return df4

def agg_buses_only(df: pd.DataFrame) -> pd.DataFrame:
    """
    filters df to only include projects with bus procurement. 
    does not include engineering, planning or construction only projects.
    then, aggregates the df by agency name and ppno. Agencies may have multiple projects that procure different types of buses
    """
    df2 = df[df["bus_count"] > 0]
    
    df3 = (
        df2.groupby(
            [
                "grant_recipient",
                "ppno",
                "prop_type",
                "bus_size_type",
            ]
        )
        .agg({"total_project_cost": "sum", "bus_count": "sum"})
        .reset_index()
    )
    return df3

if __name__ == "__main__":
    
    # initial df
    df1 = clean_tircp_columns()
    
    # aggregate 
    df2 = agg_buses_only(df1)
    
    # export both df's as parquets to GCS
    df1.to_parquet(f'{gcs_path}clean_tircp_project.parquet')
    df2.to_parquet(f'{gcs_path}clean_tircp_project_bus_only.parquet')