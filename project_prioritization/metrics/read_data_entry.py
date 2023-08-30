# Read In CSIS Metrics Testing Data Entry
# Process data entry tabs and save as parquets

# header info
import pandas as pd
import numpy as np
from shared_utils import utils
pd.options.display.max_columns = 100
import gcsfs
from calitp_data_analysis.sql import to_snakecase

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_prioritization/"

# safety
safety = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}Metrics_Scoring_All_Projects.xlsx', sheet_name="Safety"))
# fix save error due to spaces in blank values: replace field that's entirely space (or empty) with NaN
safety=safety.replace(r'^\s*$', np.nan, regex=True)
safety = safety.astype({'crf_1':'float','crf_2':'float'})
safety.to_parquet(f'{GCS_FILE_PATH}data_entry_raw_safety.parquet')

# DAC Traffic Impacts
dac_traffic = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}Metrics_Scoring_All_Projects.xlsx', sheet_name="DAC Traffic Impacts"))
dac_traffic.to_parquet(f'{GCS_FILE_PATH}data_entry_raw_dac_traffic.parquet')

# Land use
land_use = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}Metrics_Scoring_All_Projects.xlsx', sheet_name="Land Use"))
land_use.columns = land_use.columns.str.replace('?', '')
land_use.to_parquet(f'{GCS_FILE_PATH}data_entry_raw_land_use.parquet')

# VMT
vmt = to_snakecase(pd.read_excel(f'{GCS_FILE_PATH}Metrics_Scoring_All_Projects.xlsx', sheet_name="VMT"))
vmt.columns = vmt.columns.str.replace('?', '')
vmt['estimated_change_in_vmt__total_for_project_'] = vmt['estimated_change_in_vmt__total_for_project_'].str.replace('\n', '')
vmt.to_parquet(f'{GCS_FILE_PATH}data_entry_raw_vmt.parquet')

