# all functions used for annual ridership report
import os
import shutil
import sys
# import annual_ridership_module
sys.path.append("../monthly_ridership_report") # for update_var
sys.path.append("../") # for module
import pandas as pd
from siuba import _, collect, count, filter, select, show_query
from calitp_data_analysis.tables import tbls
from update_vars import GCS_FILE_PATH, NTD_MODES, NTD_TOS, MONTH, YEAR
from segment_speed_utils.project_vars import PUBLIC_GCS
import _01_ntd_ridership_utils
import gcsfs
fs = gcsfs.GCSFileSystem()
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"

  
if __name__ == "__main__":
    min_year=2018
    annual_col_dict= {
    "source_agency":"agency",
    "service":"tos"
    }

    annual_cover_sheet_path = "annual_report_cover_sheet_template.xlsx"
    annual_index_col = "**NTD Annual Ridership by RTPA**"
    annual_data_file_name = f"{YEAR}_{MONTH}_annual_report_data"
    
    print("produce annual ntd ridership data")
    df = _01_ntd_ridership_utils.produce_annual_ntd_ridership_data_by_rtpa(
        min_year=min_year, 
        split_scag=True)
    print("saving parqut to private GCS")
    
    df.to_parquet(f"{GCS_FILE_PATH}annual_ridership_report_data.parquet")

    os.makedirs(f"./{YEAR}_{MONTH}/")
    
    print("saving RTPA outputs")
    _01_ntd_ridership_utils.save_rtpa_outputs(
        df=df, 
        year = YEAR, 
        month = MONTH,
        col_dict = annual_col_dict,
        cover_sheet_path = annual_cover_sheet_path,
        cover_sheet_index_col = annual_index_col,
        output_file_name = annual_data_file_name,
        report_type = "annual"
    )
    
    print("removing local folder")
    _01_ntd_ridership_utils.remove_local_outputs(YEAR, MONTH)
    
    print("complete")
    
