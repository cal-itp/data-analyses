from shared_utils import rt_dates, rt_utils
from bus_service_utils import utils

BUS_SERVICE_GCS = f"{utils.GCS_FILE_PATH}"
COMPILED_CACHED_GCS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"
CURRENT_QUARTER = "Q4_2022"
ANALYSIS_DATE = rt_dates.PMAC[CURRENT_QUARTER] 


def get_filename(
    file_name_prefix: str, 
    analysis_date: str, 
    warehouse_version: str
) -> str:
    if analysis_date.startswith("2022"):
        file_name = f"{file_name_prefix}{analysis_date}_{warehouse_version}.parquet"
    else:
        file_name = f"{file_name_prefix}{analysis_date}.parquet"

    return file_name