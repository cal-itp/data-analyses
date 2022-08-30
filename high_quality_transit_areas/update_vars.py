import datetime as dt
from shared_utils import rt_utils, rt_dates

analysis_date = rt_dates.DATES["aug2022"] 

CACHED_VIEWS_EXPORT_PATH = f"{rt_utils.GCS_FILE_PATH}cached_views/"
ALL_OPERATORS_FILE = "./hqta_operators.json"
VALID_OPERATORS_FILE = "./valid_hqta_operators.json"