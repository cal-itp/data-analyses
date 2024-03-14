from pathlib import Path
from shared_utils import rt_dates

analysis_date = rt_dates.DATES["mar2024"]

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"
TRAFFIC_OPS_GCS = f"{GCS_FILE_PATH}traffic_ops/"
HQTA_GCS = f"{GCS_FILE_PATH}high_quality_transit_areas/"
SEGMENT_GCS = f"{GCS_FILE_PATH}rt_segment_speeds/"

ESRI_BASE_URL = "https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/"
XML_FOLDER = Path("xml")
DEFAULT_XML_TEMPLATE = XML_FOLDER.joinpath(Path("default_pro.xml"))
META_JSON = Path("metadata.json")
DATA_DICT_YML = Path("data_dictionary.yml")

RUN_ME = [
    "ca_hq_transit_areas", 
    "ca_hq_transit_stops",
    "ca_transit_routes", 
    "ca_transit_stops",
    "speeds_by_stop_segments", 
    "speeds_by_route_time_of_day",
]