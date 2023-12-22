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

fs = gcsfs.GCSFileSystem()
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"

RTPA_URL = ("https://services3.arcgis.com/bWPjFyq029ChCGur/arcgis/rest/services/"
       "RTPAs/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
      )

#gpd.read_file(RTPA_URL).RTPA.drop_duplicates().to_csv("rtpa.csv")
def save_rtpa_outputs(df: pd.DataFrame, year: int, month: str):
    """
    Export a csv for each RTPA into a folder.
    Zip that folder. 
    Upload zipped file to GCS.
    """
    for i in df.RTPA.unique():
        # Filename should be snakecase
        rtpa_snakecase = i.replace(' ', '_').lower()

        (df[df.RTPA == i]
         .sort_values("NTD ID")
         .drop(columns = "_merge")
         .to_csv(
            f"{GCS_FILE_PATH}{year}_{month}/{rtpa_snakecase}.csv",
            index = False)
        )
       
    # Zip this folder, and save zipped output to GCS
    shutil.make_archive(f"./{year}_{month}", "zip", f"{year}_{month}")
    print("Zipped folder")
    
    fs.upload(
        f"./{year}_{month}.zip", 
        f"{GCS_FILE_PATH}{year}_{month}.zip"
    )
    
    print("Uploaded to GCS")
    
    return


def produce_ntd_monthly_ridership_by_rtpa(
    upt_url: str,
    year: int,
    month: str
) -> pd.DataFrame:
    """
    Import NTD data from url, filter to CA, 
    merge in crosswalk, and save individual csvs.
    """
    # Import data, make sure NTD ID is string
    full_upt = pd.read_excel(
        upt_url, sheet_name = "UPT", 
        dtype = {"NTD ID": "str"}
    )

    full_upt = full_upt[full_upt.Agency.notna()].reset_index(drop=True)
    full_upt.to_parquet(
        f"{GCS_FILE_PATH}ntd_monthly_ridership_{year}_{month}.parquet"
    )
    
    # Filter to CA
    ca = full_upt[(full_upt["UZA Name"].str.contains(", CA")) & 
            (full_upt.Agency.notna())].reset_index(drop=True)
    
    crosswalk = pd.read_csv(
        f"{GCS_FILE_PATH}ntd_id_rtpa_crosswalk.csv", 
        dtype = {"NTD ID": "str"}
    )
    
    df = pd.merge(
        ca,
        # Merging on too many columns can create problems 
        # because csvs and dtypes aren't stable / consistent 
        # for NTD ID, Legacy NTD ID, and UZA
        crosswalk[["NTD ID", "RTPA"]],
        on = "NTD ID",
        how = "left",
        indicator = True
    )
    
    print(df._merge.value_counts())
    
    # Good, everything merged, as we want
    if len(df[df._merge=="left_only"]) > 0:
        raise ValueError("There are unmerged rows to crosswalk")
    
    # For each RTPA, we'll produce a single csv and save it to a local folder
    os.makedirs(f"./{year}_{month}/")
    
    save_rtpa_outputs(df, year, month)
    
    return

def remove_local_outputs(year: int, month: str):
    shutil.rmtree(f"{year}_{month}/")
    os.remove(f"{year}_{month}.zip")
    
    
if __name__ == "__main__":
    
    # Define variables we'll probably change later
    YEAR = "2023"
    MONTH = "October"
    MONTH_CREATED = "2023-12"

    FULL_URL = (
        "https://www.transit.dot.gov/sites/fta.dot.gov/files/"
        f"{MONTH_CREATED}/{MONTH}%20{YEAR}%20"
        "Complete%20Monthly%20Ridership%20%28with%20"
        "adjustments%20and%20estimates%29.xlsx"
    )
    
    produce_ntd_monthly_ridership_by_rtpa(FULL_URL, YEAR, MONTH)
    remove_local_outputs(YEAR, MONTH)