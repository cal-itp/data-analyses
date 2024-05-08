from shared_utils import rt_dates, rt_utils
from bus_service_utils import utils

BUS_SERVICE_GCS = f"{utils.GCS_FILE_PATH}"
COMPILED_CACHED_GCS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"
CURRENT_QUARTER = "Q2_2024"
ANALYSIS_DATE = rt_dates.PMAC[CURRENT_QUARTER] 