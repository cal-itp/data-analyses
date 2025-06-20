import gcsfs
import geopandas as gpd
import os
import pandas as pd
import shutil

from calitp_data_analysis.tables import tbls
from siuba import _, collect, count, filter, show_query, select, distinct
from calitp_data_analysis.sql import to_snakecase
from segment_speed_utils.project_vars import PUBLIC_GCS
from update_vars import GCS_FILE_PATH, NTD_MODES, NTD_TOS


fs = gcsfs.GCSFileSystem()

RTPA_URL = ("https://services3.arcgis.com/bWPjFyq029ChCGur/arcgis/rest/services/"
       "RTPAs/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
      )

#gpd.read_file(RTPA_URL).RTPA.drop_duplicates().to_csv("rtpa.csv")
def add_change_columns(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    This function works with the warehouse `dim_monthly_ntd_ridership_with_adjustments` long data format.
    Sorts the df by ntd id, mode, tos, period month and period year. then adds 2 new columns, 1. previous year/month UPT and 2. UPT change 1yr.
    """

    sort_cols2 =  ["ntd_id","mode", "tos","period_month", "period_year"] # got the order correct with ["period_month", "period_year"]! sorted years with grouped months
    group_cols2 = ["ntd_id","mode", "tos"]
    
    df[["period_year","period_month"]] = df[["period_year","period_month"]].astype(int)

    df = df.assign(
        previous_y_m_upt = (df.sort_values(sort_cols2)
                        .groupby(group_cols2)["upt"] 
                        .apply(lambda x: x.shift(1))
                       )
    )

    df["change_1yr"] = (df["upt"] - df["previous_y_m_upt"])
    
    df = get_percent_change(df)
    
    return df


def get_percent_change(
    df: pd.DataFrame, 
) -> pd.DataFrame:
    """
    updated to work with the warehouse `dim_monthly_ntd_ridership_with_adjustments` long data format. 
    
    """
    df["pct_change_1yr"] = (
        (df["upt"] - df["previous_y_m_upt"])
        .divide(df["upt"])
        .round(4)
    )
    
    return df


def sum_by_group(
    df: pd.DataFrame,
    group_cols: list) -> pd.DataFrame:
    """
    since data is now long to begin with, this replaces old sum_by_group, make_long and assemble_long_df functions.
    """
    grouped_df = df.groupby(group_cols+
                             ['period_year',
                             'period_month',
                             'period_year_month']
                           ).agg({
        "upt":"sum",
        "previous_y_m_upt":"sum",
        "change_1yr":"sum"
    }
    ).reset_index()
    
    #get %change back
    grouped_df = get_percent_change(grouped_df)
    
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
    
    # Get agencies and RTPA name
    ntd_rtpa_orgs = (
        tbls.mart_transit_database.dim_organizations()
        >> filter(
            _._is_current == True,
            _.ntd_id_2022.notna(),
            _.rtpa_name.notna(),
        )
        >> select(
            _.name, 
            _.ntd_id_2022, 
            _.rtpa_name, 
            _.mpo_name, 
            _.key
        )
        >> collect()
    )

    # join bridge org county geo to get agency counties
    bridge_counties = (
        tbls.mart_transit_database.bridge_organizations_x_headquarters_county_geography()
        >> filter(
            _._is_current == True
        )
        >> select(
            _.county_geography_name, 
            _.organization_key
        )
        >> collect()
    )
    
    # merge to get crosswalk
    ntd_to_rtpa_crosswalk = ntd_rtpa_orgs.merge(
        bridge_counties, 
        left_on="key", 
        right_on="organization_key", 
        how="left"
    )
    
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
    upload_to_public: bool = False
):
    """
    Export an excel for each RTPA, adds a READ ME tab, then writes into a folder.
    Zip that folder. 
    Upload zipped file to GCS.
    """
    col_dict ={
    'Uace Cd': "UACE Code",
    'Dt': "Date",
    'Ntd Id': "NTD ID",
    'Tos': "Type of Service",
    'Legacy Ntd Id': "Legacy NTD ID",
    'Upt': "UPT",
    'Vrm': "VRM",
    'Vrh': "VRH",
    'Voms': "VOMS",
    'Rtpa': "RTPA",
    'Previous Y M Upt': "Previous Year/Month UPT",
    'Change 1Yr': "Change in 1 Year UPT",
    'Pct Change 1Yr': "Percent Change in 1 Year UPT",
    'Tos Full': "Type of Service Full Name"
}
    print("creating individual RTPA excel files")
    
    for i in df["rtpa_name"].unique():
        
        print(f"creating excel file for: {i}")
        
        # Filename should be snakecase
        rtpa_snakecase = i.replace(' ', '_').lower()
        
        #insertng readme cover sheet, 
        cover_sheet = pd.read_excel("./cover_sheet_template.xlsx", index_col = "**NTD Monthly Ridership by RTPA**")
        cover_sheet.to_excel(
            f"./{year}_{month}/{rtpa_snakecase}.xlsx", sheet_name = "README")

        rtpa_data =(df[df["rtpa_name"] == i]
         .sort_values("ntd_id")
         #got error from excel not recognizing timezone, made list to include dropping "execution_ts" column
         .drop(columns = "_merge")
         #cleaning column names
         .rename(columns=lambda x: x.replace("_"," ").title().strip())
         #rename columns
         .rename(columns=col_dict)
                   )
        #column lists for aggregations
        agency_cols = ["ntd_id", "agency", "rtpa_name"]
        mode_cols = ["mode", "rtpa_name"]
        tos_cols = ["tos", "rtpa_name"]

        # Creating aggregations
        by_agency_long = sum_by_group((df[df["rtpa_name"] == i]), agency_cols)                                 
        by_mode_long = sum_by_group((df[df["rtpa_name"] == i]), mode_cols)
        by_tos_long = sum_by_group((df[df["rtpa_name"] == i]), tos_cols)
        
        #writing pages to excel fil
        with pd.ExcelWriter(f"./{year}_{month}/{rtpa_snakecase}.xlsx", mode ="a") as writer:
            rtpa_data.to_excel(writer, sheet_name = "RTPA Ridership Data", index=False)
            by_agency_long.to_excel(writer, sheet_name = "Aggregated by Agency", index=False)
            by_mode_long.to_excel(writer, sheet_name = "Aggregated by Mode", index=False)
            by_tos_long.to_excel(writer, sheet_name = "Aggregated by TOS", index=False)
    
    print("zipping all excel files")
    
    shutil.make_archive(f"./{year}_{month}", "zip", f"{year}_{month}")
    
    print("Zipped folder")
    
    fs.upload(
        f"./{year}_{month}.zip", 
        f"{GCS_FILE_PATH}{year}_{month}.zip"
    )
    
    if upload_to_public:
        fs.upload(
            f"./{year}_{month}.zip",
            f"{PUBLIC_GCS}ntd_monthly_ridership/{year}_{month}.zip"
        )
    
    print("Uploaded to GCS")
    
    return


# updated
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

    # full_upt.to_parquet(
    #     f"{GCS_FILE_PATH}ntd_monthly_ridership_{year}_{month}.parquet"
    # )

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
    
    df = add_change_columns(df)
    
    df = df.assign(
        Mode_full = df["mode"].map(NTD_MODES),
        TOS_full = df["tos"].map(NTD_TOS)
    )
    
    return df

def produce_annual_ntd_ridership_data_by_rtpa(min_year: str, split_scag: bool) -> pd.DataFrame:
    """
    Function that ingest time series ridership data from `mart_ntd_funding_and_expenses.fct_service..._by_mode_upt`. 
    Filters for CA agencies with last report year and year of data greater than min_year
    Merges in ntd_id_to_rtpa_crosswalk function. Aggregates by agency, mode and TOS. calculates change in UPT.
    """
    from annual_ridership_module import add_change_columns
    
    
    print("ingest annual ridership data from warehouse")
    
    ntd_service =(
        tbls.mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt()
        >> filter(
            _.year >= min_year,
            _.last_report_year >= min_year,
            _.primary_uza_name.str.contains(", CA") | 
            _.primary_uza_name.str.contains("CA-NV") |
            _.primary_uza_name.str.contains("California Non-UZA") 
        )
        >> select(
            'source_agency',
            'agency_status',
            'legacy_ntd_id',
            'last_report_year',
            'mode',
            'ntd_id',
            'reporter_type',
            'reporting_module',
            'service',
            'uace_code',
            'primary_uza_name',
            'uza_population',
            'year',
            'upt',
        )
        >> collect())
    
    ntd_service = (
        ntd_service.groupby(
            [
                "source_agency",
                "agency_status",
                #"city",
                #"state",
                "ntd_id",
                "primary_uza_name",
                "reporter_type",
                "mode",
                "service",
                "last_report_year",
                "year",
            ]
        )
        .agg({"upt": "sum"})
        .sort_values(by="ntd_id")
        .reset_index()
    )
    
    print("create crosswalk from ntd_id_to_rtpa_crosswalk function")
    
    # Creating crosswalk using function, enable splitting scag to indivdual CTC
    ntd_to_rtpa_crosswalk = ntd_id_to_rtpa_crosswalk(split_scag=split_scag)
    
    
    print("merge ntd data to crosswalk")
    # merge service data to crosswalk
    ntd_data_by_rtpa = ntd_service.merge(
        ntd_to_rtpa_crosswalk,
        how="left",
        left_on=[
            "ntd_id",
            # "agency", "reporter_type", "city" # sometime agency name, reporter type and city name change or are inconsistent, causing possible fanout
        ],
        right_on="ntd_id_2022",
        indicator=True,
    )
    
    # list of ntd_id with LA County Dept of Public Works name
    lacdpw_list = [
        "90269",
        "90270",
        "90272",
        "90273",
        "90274",
        "90275",
        "90276",
        "90277",
        "90278",
        "90279",
    ]
    
    # replace LA County Public Works agencies with their own RTPA
    ntd_data_by_rtpa.loc[
        ntd_data_by_rtpa["ntd_id"].isin(lacdpw_list), ["rtpa_name", "_merge"]
    ] = ["Los Angeles County Department of Public Works", "both"]
    
    print(ntd_data_by_rtpa._merge.value_counts())
        
    if len(ntd_data_by_rtpa[ntd_data_by_rtpa._merge=="left_only"]) > 0:
        raise ValueError("There are unmerged rows to crosswalk")
    
    print("add `change_column` to data")
    ntd_data_by_rtpa = annual_ridership_module.add_change_columns(ntd_data_by_rtpa)
    
    print("map mode and tos desc.")
    ntd_data_by_rtpa = ntd_data_by_rtpa.assign(
        mode_full = ntd_data_by_rtpa["mode"].map(NTD_MODES),
        service_full = ntd_data_by_rtpa["service"].map(NTD_TOS)
    )
    print("complete")
    return ntd_data_by_rtpa

def remove_local_outputs(
    year: int, 
    month: str
):
    shutil.rmtree(f"{year}_{month}/")
    os.remove(f"{year}_{month}.zip")