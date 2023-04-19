import geopandas as gpd
import pandas as pd
from calitp_data_analysis.sql import to_snakecase
import datetime

import os
os.environ['USE_PYGEOS'] = '0'
import geopandas

from segment_speed_utils.project_vars import analysis_date
from segment_speed_utils import helpers
from shared_utils import  dask_utils, geography_utils, utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
SHARED_GCS = f"{GCS_FILE_PATH}shared_data/"

