"""
Set up variables for 100 bus opportunities analysis
"""
from shared_utils import rt_dates, rt_utils
from bus_service_utils import utils

ANALYSIS_DATE = rt_dates.DATES["may2022"]
GCS_FILE_PATH = utils.GCS_FILE_PATH
COMPILED_CACHED_GCS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"