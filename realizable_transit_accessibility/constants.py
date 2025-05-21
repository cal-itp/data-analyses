from shared_utils import catalog_utils

WAREHOUSE_DATE_STRFTIME = "%Y-%m-%d"
GTFS_DATE_STRFTIME = "%Y%m%d"

ARBITRARY_SERVICE_ID = "0"

RT_COLUMN_RENAME_MAP = {
    "stop_id": "warehouse_stop_id",
    "scheduled_arrival_sec": "warehouse_scheduled_arrival_sec",
}

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")