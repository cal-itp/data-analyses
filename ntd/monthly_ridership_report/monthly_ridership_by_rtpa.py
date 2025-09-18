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
import sys

sys.path.append("../")  # up one level

from calitp_data_analysis.sql import to_snakecase
from segment_speed_utils.project_vars import PUBLIC_GCS
from update_vars import GCS_FILE_PATH, NTD_MODES, NTD_TOS, MONTH, YEAR
import _01_ntd_ridership_utils #import produce_ntd_monthly_ridership_by_rtpa, save_rtpa_outputs

fs = gcsfs.GCSFileSystem()

RTPA_URL = ("https://services3.arcgis.com/bWPjFyq029ChCGur/arcgis/rest/services/"
       "RTPAs/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
      )

    
if __name__ == "__main__":
    
    monthly_col_dict ={
        'Uace Cd': "UACE Code",
        'Dt': "Date",
        'Tos': "Type of Service",
        'Legacy Ntd Id': "Legacy NTD ID",
        'Vrm': "VRM",
        'Vrh': "VRH",
        'Voms': "VOMS",
        'Rtpa': "RTPA",
        'Pct Change 1Yr': "Percent Change in 1 Year UPT",
        'Tos Full': "Type of Service Full Name"
    }

    monthly_cover_sheet_path = "cover_sheet_template.xlsx"
    monthly_index_col = "**NTD Monthly Ridership by RTPA**"
    monthly_data_file_name = f"{YEAR}_{MONTH}_monthly_report_data"
    
    
    df = _01_ntd_ridership_utils.produce_ntd_monthly_ridership_by_rtpa(YEAR, MONTH)
    print(df.columns)
    df.to_parquet(f"{GCS_FILE_PATH}ca_monthly_ridership_{YEAR}_{MONTH}.parquet")
    
    # For each RTPA, we'll produce a single excel and save it to a local folder
    os.makedirs(f"./{YEAR}_{MONTH}/")
    
    df = pd.read_parquet(
        f"{GCS_FILE_PATH}ca_monthly_ridership_{YEAR}_{MONTH}.parquet"
    )
    print("execute save_rtpa_outputs")
    _01_ntd_ridership_utils.save_rtpa_outputs(
        df= df, 
        year = YEAR, 
        month = MONTH,
        #col_dict = monthly_col_dict,
        cover_sheet_path = monthly_cover_sheet_path,
        cover_sheet_index_col = monthly_index_col,
        output_file_name = monthly_data_file_name,
        report_type = "monthly",
        monthly_upload_to_public= True
    )
    
    print("execute remove_local_outputs")
    _01_ntd_ridership_utils.remove_local_outputs(YEAR, MONTH)
    
    print("complete")