"""
NTD Monthly Ridership by RTPA

1. Transit operators (`ntd_id`) in CA should be associated with RTPAs (use crosswalk uploaded in GCS)
2. For each RTPA, grab the latest month's ridership column, sort transit operators alphabetically, and write out spreadsheets.
3. Spreadsheets stored in folder to send to CalSTA.
"""
import gcsfs
import geopandas as gpd
import os
import pandas as pd
import shutil

from calitp_data_analysis.tables import tbls
from siuba import _, collect, count, filter, show_query
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
    
    for i in df["RTPA"].unique():
        # Filename should be snakecase
        rtpa_snakecase = i.replace(' ', '_').lower()

        (df[df["RTPA"] == i]
         .sort_values("ntd_id")
         #got error from excel not recognizing timezone, made list to include dropping "ts" column
         .drop(columns = ["_merge","ts"])
         #cleaning column names
         .rename(columns=lambda x: x.replace("_"," ").title().strip())
         #rename columns
         .rename(columns=col_dict)
         #updated to `to_excel`, added sheet_name 
         .to_excel(
            f"./{year}_{month}/{rtpa_snakecase}.xlsx", sheet_name = "RTPA Ridership Data",
            index = False)
         
        )
        #insertng readme cover sheet, 
        cover_sheet = pd.read_excel("./cover_sheet_template.xlsx", index_col = "NTD Monthly Ridership by RTPA")
        with pd.ExcelWriter(f"./{year}_{month}/{rtpa_snakecase}.xlsx", mode ="a") as writer:
            cover_sheet.to_excel(writer, sheet_name = "READ ME")
        
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


def produce_ntd_monthly_ridership_by_rtpa(
    year: int,
    month: int
) -> pd.DataFrame:
    """
    This function works with the warehouse `dim_monthly_ntd_ridership_with_adjustments` long data format.
    Import NTD data from warehouse, filter to CA, 
    merge in crosswalk, checks for unmerged rows, then creates new columns for full Mode and TOS name.
    
    """
    full_upt = (tbls.mart_ntd.dim_monthly_ntd_ridership_with_adjustments() >> collect()).rename(columns = {"mode_type_of_service_status": "Status"})
    
    full_upt = full_upt[full_upt.agency.notna()].reset_index(drop=True)
    
    full_upt.to_parquet(
        f"{GCS_FILE_PATH}ntd_monthly_ridership_{year}_{month}.parquet"
    )
    
    ca = full_upt[(full_upt["uza_name"].str.contains(", CA")) & 
            (full_upt.agency.notna())].reset_index(drop=True)
    
    crosswalk = pd.read_csv(
        f"gs://calitp-analytics-data/data-analyses/ntd/ntd_id_rtpa_crosswalk.csv", 
        dtype = {"NTD ID": "str"}
    #have to rename NTD ID col to match the dim table
    ).rename(columns={"NTD ID": "ntd_id"})
    
    df = pd.merge(
        ca,
        # Merging on too many columns can create problems 
        # because csvs and dtypes aren't stable / consistent 
        # for NTD ID, Legacy NTD ID, and UZA
        crosswalk[["ntd_id", "RTPA"]],
        on = "ntd_id",
        how = "left",
        indicator = True
    )
    
    print(df._merge.value_counts())
    
    if len(df[df._merge=="left_only"]) > 0:
        raise ValueError("There are unmerged rows to crosswalk")
    
    df = add_change_columns(df)
    
    df = df.assign(
        Mode_full = df["mode"].map(NTD_MODES),
        TOS_full = df["tos"].map(NTD_TOS)
    )
    
    return df


def remove_local_outputs(
    year: int, 
    month: str
):
    shutil.rmtree(f"{year}_{month}/")
    os.remove(f"{year}_{month}.zip")
    
    
if __name__ == "__main__":
    
    # Define variables we'll probably change later
    from update_vars import YEAR, MONTH
    
    df = produce_ntd_monthly_ridership_by_rtpa(YEAR, MONTH)
    print(df.columns)
    df.to_parquet(f"{GCS_FILE_PATH}ca_monthly_ridership_{YEAR}_{MONTH}.parquet")
    
    # For each RTPA, we'll produce a single excel and save it to a local folder
    os.makedirs(f"./{YEAR}_{MONTH}/")
    
    df = pd.read_parquet(
        f"{GCS_FILE_PATH}ca_monthly_ridership_{YEAR}_{MONTH}.parquet"
    )
    save_rtpa_outputs(df, YEAR, MONTH, upload_to_public = True)
    remove_local_outputs(YEAR, MONTH)
    