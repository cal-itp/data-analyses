from pathlib import Path
from shared_utils import catalog_utils, rt_dates

analysis_date = rt_dates.DATES["sep2024"]

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

COMPILED_CACHED_VIEWS = GTFS_DATA_DICT.gcs_paths.COMPILED_CACHED_VIEWS
SEGMENT_GCS = GTFS_DATA_DICT.gcs_paths.SEGMENT_GCS
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS
SCHED_GCS = GTFS_DATA_DICT.gcs_paths.SCHED_GCS
TRAFFIC_OPS_GCS = f"{GTFS_DATA_DICT.gcs_paths.GCS}traffic_ops/"
HQTA_GCS = f"{GTFS_DATA_DICT.gcs_paths.GCS}high_quality_transit_areas/"

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