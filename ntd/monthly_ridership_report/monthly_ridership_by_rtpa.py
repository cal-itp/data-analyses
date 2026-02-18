"""
NTD Monthly Ridership by RTPA

1. Transit operators (`ntd_id`) in CA should be associated with RTPAs (use crosswalk uploaded in GCS)
2. For each RTPA, grab the latest month's ridership column, sort transit operators alphabetically, and write out spreadsheets.
3. Spreadsheets stored in folder to send to CalSTA.
"""

import importlib
import os
import sys
from functools import cache

import gcsfs
from calitp_data_analysis.gcs_pandas import GCSPandas

sys.path.append(os.path.abspath("../monthly_ridership_report"))  # for update_var
sys.path.append(os.path.abspath("../"))  # for module
_01_ntd_ridership_utils = importlib.import_module("_01_ntd_ridership_utils")
update_vars = importlib.import_module("update_vars")


@cache
def gcs_pandas():
    return GCSPandas()


fs = gcsfs.GCSFileSystem()
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"

RTPA_URL = (
    "https://services3.arcgis.com/bWPjFyq029ChCGur/arcgis/rest/services/"
    "RTPAs/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
)


if __name__ == "__main__":

    monthly_col_dict = {
        "Uace Cd": "UACE Code",
        "Dt": "Date",
        "Tos": "Type of Service",
        "Legacy Ntd Id": "Legacy NTD ID",
        "Vrm": "VRM",
        "Vrh": "VRH",
        "Voms": "VOMS",
        "Rtpa": "RTPA",
        "Pct Change 1Yr": "Percent Change in 1 Year UPT",
        "Tos Full": "Type of Service Full Name",
    }

    monthly_cover_sheet_path = "cover_sheet_template.xlsx"
    monthly_index_col = "**NTD Monthly Ridership by RTPA**"
    monthly_data_file_name = f"{update_vars.YEAR}_{update_vars.MONTH}_monthly_report_data"

    df = _01_ntd_ridership_utils.produce_ntd_monthly_ridership_by_rtpa(update_vars.YEAR, update_vars.MONTH)
    print(df.columns)
    gcs_pandas().data_frame_to_parquet(
        df, f"{GCS_FILE_PATH}ca_monthly_ridership_{update_vars.YEAR}_{update_vars.MONTH}.parquet"
    )

    # For each RTPA, we'll produce a single excel and save it to a local folder
    os.makedirs(f"./{update_vars.YEAR}_{update_vars.MONTH}/")

    df = gcs_pandas().read_parquet(
        f"{GCS_FILE_PATH}ca_monthly_ridership_{update_vars.YEAR}_{update_vars.MONTH}.parquet"
    )
    print("execute save_rtpa_outputs")
    _01_ntd_ridership_utils.save_rtpa_outputs(
        df=df,
        year=update_vars.YEAR,
        month=update_vars.MONTH,
        # col_dict = monthly_col_dict,
        cover_sheet_path=monthly_cover_sheet_path,
        cover_sheet_index_col=monthly_index_col,
        output_file_name=monthly_data_file_name,
        report_type="monthly",
        monthly_upload_to_public=True,
    )

    print("execute remove_local_outputs")
    _01_ntd_ridership_utils.remove_local_outputs(update_vars.YEAR, update_vars.MONTH)

    print("complete")
