from shared_utils import rt_dates, rt_utils

SELECTED_DATE = rt_dates.DATES["sep2022"]
COMPILED_CACHED_VIEWS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"
GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/regional_bus_network/'