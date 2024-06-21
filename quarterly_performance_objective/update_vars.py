from shared_utils import catalog_utils, rt_dates

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

GCS_FILE_PATH = GTFS_DATA_DICT.gcs_paths.GCS
BUS_SERVICE_GCS = f"{GCS_FILE_PATH}bus_service_increase/"
COMPILED_CACHED_GCS = GTFS_DATA_DICT.gcs_paths.COMPILED_CACHED_VIEWS
SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS

analysis_date = rt_dates.DATES["jun2024"]
#CURRENT_QUARTER = "Q2_2024"
#ANALYSIS_DATE = rt_dates.PMAC[CURRENT_QUARTER] 