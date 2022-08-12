import datetime as dt
from shared_utils import rt_utils

analysis_date = dt.date(2022, 7, 13) 
date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

CACHED_VIEWS_EXPORT_PATH = f"{rt_utils.GCS_FILE_PATH}cached_views/"
ALL_OPERATORS_FILE = "./hqta_operators.json"
VALID_OPERATORS_FILE = "./valid_hqta_operators.json"