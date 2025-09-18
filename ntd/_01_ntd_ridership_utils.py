"""
Fucntions for the Monthly/Annual NTD Ridership reports.
"""

import gcsfs
import geopandas as gpd
import os
import pandas as pd
import shutil

from calitp_data_analysis.sql import query_sql, to_snakecase
from segment_speed_utils.project_vars import PUBLIC_GCS
from typing import Literal

import sys
sys.path.append("./monthly_ridership_report")
from update_vars import GCS_FILE_PATH, NTD_MODES, NTD_TOS, YEAR, MONTH


fs = gcsfs.GCSFileSystem()


def add_change_columns(
    df: pd.DataFrame,
	sort_cols: str,
	group_cols: str,
	change_col: str
) -> pd.DataFrame:
    """
    This function works with the warehouse `dim_monthly_ntd_ridership_with_adjustments` long data format.
    Sorts the df by ntd id, mode, tos, period month and period year. then adds 2 new columns, 1. previous year/month UPT and 2. UPT change 1yr.
    sort_cols and group_cols args needed to specify annual/monthly specific groupings.
    change_col arg is used to name the new column.
    """
    # checks for monthly data specific columns, changes them to int.
    if {"period_year","period_month"}.issubset(df.columns):
        df[["period_year","period_month"]] = df[["period_year","period_month"]].astype(int)

    df = df.assign(
        # unpacks dictionary to use change_col arg
        **{change_col : (df.sort_values(sort_cols)
                        .groupby(group_cols)["upt"] 
                        .apply(lambda x: x.shift(1))
                        )}
    )

    df["change_1yr"] = (df["upt"] - df[change_col])
    
    df = get_percent_change(
        df,
        change_col = change_col
    )
    
    return df


def get_percent_change(
    df: pd.DataFrame,
    change_col: str
) -> pd.DataFrame:
    """
    updated to work with the warehouse `dim_monthly_ntd_ridership_with_adjustments` long data format. Used with add_change_col to make a new column to calc % change from previous period.
    
    """
    df["pct_change_1yr"] = (
        (df["upt"] - df[change_col])
        .divide(df[change_col])
        .round(4)
    )
    
    return df


def sum_by_group(
    df: pd.DataFrame,
    group_cols: list,
    group_col2: list,
    agg_cols: dict,
    change_col: str,
    
) -> pd.DataFrame:
    """
    Since data is now long to begin with, this replaces old sum_by_group, make_long and assemble_long_df functions.
    Separated col groups args so function can be used by annual/monthly report
    """
    grouped_df = df.groupby(group_cols+
                             group_col2
                           ).agg(agg_cols
    ).reset_index()
    
    #get %change back
    grouped_df = get_percent_change(grouped_df, change_col)
    
    #decimal to whole number
    grouped_df["pct_change_1yr"] = grouped_df["pct_change_1yr"]*100
    
    return grouped_df


def ntd_id_to_rtpa_crosswalk(split_scag:bool) -> pd.DataFrame:
    """
    Creates ntd_id to rtpa crosswalk. Reads in dim_orgs, merge in county data from bridge table.
    enable split_scag to separate the SCAG to individual county CTC for RTPA. disable split_scag to have all socal counties keep SCAG as RTPA
    
    """
    #split socal counties to county CTC
    socal_county_dict = {
        "Ventura": "Ventura County Transportation Commission",
        "Los Angeles": "Los Angeles County Metropolitan Transportation Authority",
        "San Bernardino": "San Bernardino County Transportation Authority",
        "Riverside": "Riverside County Transportation Commission",
        "Orange": "Orange County Transportation Authority",
        "Imperial": "Imperial County Transportation Commission"
    }
    
    ntd_rtpa_query ="""
    SELECT
      dim_org.name,
      dim_org.ntd_id_2022,
      dim_org.rtpa_name,
      dim_org.key,
      bridge.county_geography_name,
      bridge.organization_key
    FROM
      `cal-itp-data-infra.mart_transit_database.dim_organizations` AS dim_org
    LEFT JOIN
      `cal-itp-data-infra.mart_transit_database.bridge_organizations_x_headquarters_county_geography` AS bridge
    ON
      dim_org.key = bridge.organization_key
    WHERE
      dim_org._is_current is TRUE
      AND dim_org.ntd_id_2022 IS NOT NULL
      AND dim_org.rtpa_name IS NOT NULL
      AND bridge._is_current is TRUE
    """

    ntd_to_rtpa_crosswalk = query_sql(ntd_rtpa_query, as_df=True)
    
    
    # locate SoCal counties, replace initial RTPA name with dictionary.
    if split_scag == True:
        ntd_to_rtpa_crosswalk.loc[
            ntd_to_rtpa_crosswalk["county_geography_name"].isin(
                socal_county_dict.keys()
            ),
            "rtpa_name",
        ] = ntd_to_rtpa_crosswalk["county_geography_name"].map(socal_county_dict)
        
    return ntd_to_rtpa_crosswalk

def save_rtpa_outputs(
    df: pd.DataFrame, 
    year: int, 
    month: str,
    report_type: Literal["annual","monthly"],
    cover_sheet_path: str,
    cover_sheet_index_col: str,
    output_file_name: str,
    col_dict: dict = None,
    monthly_upload_to_public: bool = False,
    annual_upload_to_public: bool = False,
):
    """
    Export an excel for each RTPA, adds a READ ME tab, then writes into a folder.
    Zip that folder. 
    Upload zipped file to GCS.
    Updated Args to declare annual/monthly reports.
    """
    
    print("creating individual RTPA excel files")
    
    for i in df["rtpa_name"].unique():
        
        print(f"creating excel file for: {i}")
        
        # Filename should be snakecase
        rtpa_snakecase = i.replace(' ', '_').replace("/","_").lower()
        
        #insertng readme cover sheet, 
        cover_sheet = pd.read_excel(cover_sheet_path, index_col = cover_sheet_index_col)
        cover_sheet.to_excel(
            f"./{year}_{month}/{rtpa_snakecase}.xlsx", sheet_name = "README")

        rtpa_data = (df[df["rtpa_name"] == i]
         .sort_values("ntd_id")
         #got error from excel not recognizing timezone, made list to include dropping "execution_ts" column
         .drop(columns = "_merge")

                   )
        
        if col_dict:
            rtpa_data = rtpa_data.rename(columns=col_dict)
            
        agency_cols = ["ntd_id", "agency", "rtpa_name"]
        mode_cols = ["mode", "rtpa_name"]
        tos_cols = ["tos", "rtpa_name"]
        reporter_type = ["reporter_type", "rtpa_name"]
        
        if report_type == "monthly":
        #column lists for aggregations

            monthly_group_col_2 = [
            'period_year',
            'period_month',
            'period_year_month']

            monthly_agg_col = {
            "upt":"sum",
            "previous_y_m_upt":"sum",
            "change_1yr":"sum"
            }
            monthly_change_col ="previous_y_m_upt"

            by_agency_long = sum_by_group(
                df = rtpa_data,
                group_cols= agency_cols,
                group_col2= monthly_group_col_2,# look into combingin with base grou_cols
                agg_cols = monthly_agg_col,
                change_col= monthly_change_col
            )

            by_mode_long = sum_by_group(
                df = rtpa_data,
                group_cols= mode_cols,
                group_col2= monthly_group_col_2,# look into combingin with base grou_cols
                agg_cols = monthly_agg_col,
                change_col= monthly_change_col
            )
    
            by_tos_long = sum_by_group(
                df = rtpa_data,
                group_cols= tos_cols,
                group_col2= monthly_group_col_2,# look into combingin with base grou_cols
                agg_cols = monthly_agg_col,
                change_col= monthly_change_col
            )
        #writing pages to excel fil
            with pd.ExcelWriter(f"./{year}_{month}/{rtpa_snakecase}.xlsx", mode ="a") as writer:
                rtpa_data.to_excel(writer, sheet_name = "RTPA Ridership Data", index=False)
                by_agency_long.to_excel(writer, sheet_name = "Aggregated by Agency", index=False)
                by_mode_long.to_excel(writer, sheet_name = "Aggregated by Mode", index=False)
                by_tos_long.to_excel(writer, sheet_name = "Aggregated by TOS", index=False)


    
        if report_type == "annual":
            annual_group_col_2 = ["year"]

            annual_agg_col= {
                            "upt": "sum",
                            "previous_y_upt": "sum",
                            "change_1yr": "sum",
                        }
            annual_change_col ="previous_y_upt"
            
            by_agency_long = sum_by_group(
                df = rtpa_data,
                group_cols= agency_cols,
                group_col2= annual_group_col_2,# look into combingin with base grou_cols
                agg_cols = annual_agg_col,
                change_col = annual_change_col
            )

            by_mode_long = sum_by_group(
                df = rtpa_data,
                group_cols= mode_cols,
                group_col2= annual_group_col_2,# look into combingin with base grou_cols
                agg_cols = annual_agg_col,
                change_col = annual_change_col
            )
    
            by_tos_long = sum_by_group(
                df = rtpa_data,
                group_cols= tos_cols,
                group_col2= annual_group_col_2,# look into combingin with base grou_cols
                agg_cols = annual_agg_col,
                change_col = annual_change_col
            )
            by_reporter_type_long = sum_by_group(
                df = rtpa_data,
                group_cols= reporter_type,
                group_col2= annual_group_col_2,# look into combingin with base grou_cols
                agg_cols = annual_agg_col,
                change_col = annual_change_col
            )

  
            #writing pages to excel fil
            with pd.ExcelWriter(f"./{year}_{month}/{rtpa_snakecase}.xlsx", mode ="a") as writer:
                rtpa_data.to_excel(writer, sheet_name = "RTPA Ridership Data", index=False)
                by_agency_long.to_excel(writer, sheet_name = "Aggregated by Agency", index=False)
                by_mode_long.to_excel(writer, sheet_name = "Aggregated by Mode", index=False)
                by_tos_long.to_excel(writer, sheet_name = "Aggregated by TOS", index=False)
                by_reporter_type_long.to_excel(writer, sheet_name="Aggregate by Reporter Type", index=False)
    
    
    print("zipping all excel files")
    
    shutil.make_archive(f"./{output_file_name}", "zip", f"{year}_{month}")
    
    print("Zipped folder")
    
    fs.upload(
        f"./{output_file_name}.zip", 
        f"{GCS_FILE_PATH}{year}_{month}.zip"
    )
    
    if monthly_upload_to_public:
        fs.upload(
            f"./{output_file_name}.zip",
            f"{PUBLIC_GCS}ntd_monthly_ridership/{year}_{month}.zip"
        )
        print("Uploaded to public GCS - monthly report")
        
    if annual_upload_to_public:
        fs.upload(
            f"./{output_file_name}.zip",
            f"{PUBLIC_GCS}ntd_annual_ridership/{year}_{month}_annual_report_data.zip"
        )
    
        print("Uploaded to public GCS - annual report")
    
    print("complete")
    
    return


def produce_ntd_monthly_ridership_by_rtpa(year: int, month: int) -> pd.DataFrame:
    """
    This function works with the warehouse `dim_monthly_ntd_ridership_with_adjustments` long data format.
    Import NTD data from warehouse, filter to CA,
    merge in crosswalk, checks for unmerged rows, then creates new columns for full Mode and TOS name.

    """

    full_upt = (
        tbls.mart_ntd.dim_monthly_ridership_with_adjustments()
        >> filter(
            _.period_year.isin(
                ["2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"]
            )
        )
        >>select(
            _.ntd_id,
            _.agency,
            _.reporter_type,
            _.period_year_month,
            _.period_year,
            _.period_month,
            _.mode,
            _.tos,
            _.mode_type_of_service_status,
            _.primary_uza_name,
            _.upt
            
        )
        >> collect()
    ).rename(
        columns={
            "mode_type_of_service_status": "Status",
            "primary_uza_name": "uza_name",
        }
    )

    full_upt = full_upt[full_upt.agency.notna()].reset_index(drop=True)

    full_upt.to_parquet(
        f"{GCS_FILE_PATH}ntd_monthly_ridership_{year}_{month}.parquet"
    )

    ca = full_upt[
        (full_upt["uza_name"].str.contains(", CA")) & (full_upt.agency.notna())
    ].reset_index(drop=True)

    # use new crosswalk function
    crosswalk = ntd_id_to_rtpa_crosswalk(split_scag=True)

    min_year = 2018

    # get agencies with last report year and data after > 2018.
    last_report_year = (
        tbls.mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt()
        >> filter(
            _.year >= min_year,  # see if this changes anything
            _.last_report_year >= min_year,
            _.primary_uza_name.str.contains(", CA")
            | _.primary_uza_name.str.contains("CA-NV")
            | _.primary_uza_name.str.contains("California Non-UZA"),
        )
        >> distinct(
            "source_agency",
            #'agency_status',
            #'legacy_ntd_id',
            "last_report_year",
            #'mode',
            "ntd_id",
            #'reporter_type',
            #'reporting_module',
            #'service',
            #'uace_code',
            #'primary_uza_name',
            #'uza_population',
            #'year',
            #'upt',
        )
        >> collect()
    )

    # merge last report year to CA UPT data
    df = pd.merge(ca, last_report_year, left_on="ntd_id", right_on="ntd_id", how="inner")

    # merge crosswalk to CA last report year
    df = pd.merge(
        df,
        # Merging on too many columns can create problems
        # because csvs and dtypes aren't stable / consistent
        # for NTD ID, Legacy NTD ID, and UZA
        crosswalk[["ntd_id_2022", "rtpa_name"]],
        left_on="ntd_id",
        right_on="ntd_id_2022",
        how="left",
        indicator=True,
    )

    print(df._merge.value_counts())

    # check for unmerged rows
    if len(df[df._merge == "left_only"]) > 0:
        raise ValueError("There are unmerged rows to crosswalk")
    
    monthly_sort_cols =  [
    "ntd_id",
    "mode", 
    "tos",
    "period_month", 
    "period_year"
] # got the order correct with ["period_month", "period_year"]! sorted years with grouped months

    monthly_group_cols = [
        "ntd_id",
        "mode", 
        "tos"
                  ]

    monthly_change_col ="previous_y_m_upt"

    df = add_change_columns(
        df,
        sort_cols = monthly_sort_cols,
        group_cols = monthly_group_cols,
        change_col = monthly_change_col
    )

    
    df = df.assign(
        Mode_full = df["mode"].map(NTD_MODES),
        TOS_full = df["tos"].map(NTD_TOS)
    )
    
    return df



def produce_ntd_monthly_ridership_by_rtpa(year: int, month: int) -> pd.DataFrame:
    """
    This function works with the warehouse `dim_monthly_ntd_ridership_with_adjustments` long data format.
    Import NTD data from warehouse, filter to CA,
    merge in crosswalk, checks for unmerged rows, then creates new columns for full Mode and TOS name.

    """
    monthly_query ="""
    SELECT 
      ntd_id,
      agency,
      reporter_type,
      period_year_month,
      period_year,
      period_month,
      mode,
      tos,
      mode_type_of_service_status AS Status,
      primary_uza_name as uza_name,
      upt
    FROM
      `cal-itp-data-infra.mart_ntd.dim_monthly_ridership_with_adjustments`
    WHERE
      period_year IN ("2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025")
      AND agency IS NOT NULL
    """
    full_upt = query_sql(monthly_query, as_df=True)

    full_upt.to_parquet(
        f"{GCS_FILE_PATH}ntd_monthly_ridership_{year}_{month}.parquet"
    )

    ca = full_upt[
        (full_upt["uza_name"].str.contains(", CA")) & (full_upt.agency.notna())
    ].reset_index(drop=True)

    # use new crosswalk function
    crosswalk = ntd_id_to_rtpa_crosswalk(split_scag=True)

    min_year = 2018

    # get agencies with last report year and data after > 2018.
    last_report_query = """
    SELECT DISTINCT
      source_agency,
      last_report_year,
      ntd_id,
    FROM
      `cal-itp-data-infra.mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt`
    WHERE
      year >= 2018
      AND last_report_year >= 2018
      AND (
        primary_uza_name LIKE "%, CA%"
        OR primary_uza_name LIKE "%CA-NV%"
        OR primary_uza_name LIKE "%California Non-UZA%"
      )
    """

    last_report_year = query_sql(last_report_query, as_df=True)

    # merge last report year to CA UPT data
    df = pd.merge(ca, last_report_year, left_on="ntd_id", right_on="ntd_id", how="inner")
    # merge crosswalk to CA last report year
    df = pd.merge(
        df,
        # Merging on too many columns can create problems
        # because csvs and dtypes aren't stable / consistent
        # for NTD ID, Legacy NTD ID, and UZA
        crosswalk[["ntd_id_2022", "rtpa_name"]],
        left_on="ntd_id",
        right_on="ntd_id_2022",
        how="left",
        indicator=True,
    )

    print(df._merge.value_counts())

    # check for unmerged rows
    if len(df[df._merge == "left_only"]) > 0:
        raise ValueError("There are unmerged rows to crosswalk")
    
    monthly_sort_cols =  [
    "ntd_id",
    "mode", 
    "tos",
    "period_month", 
    "period_year"
] # got the order correct with ["period_month", "period_year"]! sorted years with grouped months

    monthly_group_cols = [
        "ntd_id",
        "mode", 
        "tos"
                  ]

    monthly_change_col ="previous_y_m_upt"

    df = add_change_columns(
        df,
        sort_cols = monthly_sort_cols,
        group_cols = monthly_group_cols,
        change_col = monthly_change_col
    )

    
    df = df.assign(
        Mode_full = df["mode"].map(NTD_MODES),
        TOS_full = df["tos"].map(NTD_TOS)
    )
    
    return df


def remove_local_outputs(
    year: int, 
    month: str
):
    """
    Removes YEAR_MONTH folder and the individual RTPA excel sheets, and
    deletes the YEAR_MONTH zip file from the save_rtpa_outputs function.
    """
    try:
        print("removing data folder")
        shutil.rmtree(f"{year}_{month}/")
    except FileNotFoundError:
        print ("data folder not found")
    
    if os.path.exists(f"{year}_{month}_annual_report_data.zip"):
        os.remove(f"{year}_{month}_annual_report_data.zip")
        print("removing annual data zip file")
    
    elif os.path.exists(f"{year}_{month}_monthly_report_data.zip"):
        os.remove(f"{year}_{month}_monthly_report_data.zip")
        print("removing monthly data zip file")
    
    else:
        print("Could not find report data to delete")
