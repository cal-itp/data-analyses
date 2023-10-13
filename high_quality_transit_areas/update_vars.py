from shared_utils import rt_utils, rt_dates

analysis_date = rt_utils.format_date(rt_dates.DATES["oct2023"])

CACHED_VIEWS_EXPORT_PATH = f"{rt_utils.GCS_FILE_PATH}cached_views/"
COMPILED_CACHED_VIEWS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"
TEMP_GCS = f"{rt_utils.GCS_FILE_PATH}temp/"
VALID_OPERATORS_FILE = "./valid_hqta_operators.json"