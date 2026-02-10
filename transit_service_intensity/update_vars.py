from shared_utils import rt_dates

GCS_PATH = "gs://calitp-analytics-data/data-analyses/transit_service_intensity/"

INPUT_GEOM_PATHS = {"urbanized_areas": "input_geoms/ca_uza_map.parquet"}
GEOM_INPUT_PATH = f'{GCS_PATH}{INPUT_GEOM_PATHS["urbanized_areas"]}'
GEOM_ID_COL = "uace20"  # a natural identifier for input geometries, such as a census geoid or county name
GEOM_SUBFOLDER = "urbanized_areas/"

ANALYSIS_DATE = rt_dates.DATES["jul2025"]
BORDER_BUFFER_METERS = 35
