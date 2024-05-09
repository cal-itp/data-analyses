from bus_service_utils import utils as bus_utils
from shared_utils import rt_dates

DATA_PATH = f"{bus_utils.GCS_FILE_PATH}2023_Oct/"
PUBLIC_GCS = "gs://calitp-publish-data-analysis/"

dates = {
    "wed": rt_dates.DATES["oct2023"], 
    "sat": rt_dates.DATES["oct2023e"],
    "sun": rt_dates.DATES["oct2023f"],
}
