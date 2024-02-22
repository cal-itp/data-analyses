from shared_utils import rt_utils, rt_dates

analysis_date = rt_dates.DATES["feb2024"]

COMPILED_CACHED_VIEWS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"
TEMP_GCS = f"{rt_utils.GCS_FILE_PATH}temp/"
VALID_OPERATORS_FILE = "./valid_hqta_operators.json"
PROJECT_CRS = "EPSG:3310"