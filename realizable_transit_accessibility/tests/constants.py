# Desired columns in stop_times output
TEST_STOP_TIMES_DESIRED_COLUMNS_DEFAULT = [
    "trip_id",
    "arrival_time",
    "departure_time",
    "stop_id",
    "stop_sequence",
]

TEST_STOP_TIMES_DESIRED_COLUMNS_EXTRA_COLUMNS = [
    "trip_id",
    "arrival_time",
    "drop_off_type",
    "stop_headsign",
    "stop_id",
    "departure_time",
    "stop_sequence",
]

# Collumn definitions in input data
RT_ARRIVAL_SEC = "rt_arrival_sec"
TRIP_INSTANCE_KEY = "trip_instance_key"
SCHEDULE_ARRIVAL_SEC = "schedule_arrival_sec"
STOP_SEQUENCE = "stop_sequence"
TRIP_ID = "trip_id"
STOP_ID = "stop_id"
SCHEDULE_GTFS_DATASET_KEY = "schedule_gtfs_dataset_key"

SCHEDULE_ARRIVAL_SEC_NAME = "scheduled_arrival_sec"
RT_ARRIVAL_SEC_NAME = "rt_arrival_sec"
STOP_SEQUENCE_NAME = "stop_sequence"
TRIP_INSTANCE_KEY_NAME = "trip_instance_key"
TRIP_ID_NAME = "trip_id"
STOP_ID_NAME = "stop_id"
SCHEDULE_GTFS_DATASET_KEY_NAME = "schedule_gtfs_dataset_key"

TEST_DEFAULT_COLUMN_MAP = {
    SCHEDULE_ARRIVAL_SEC: SCHEDULE_ARRIVAL_SEC_NAME,
    RT_ARRIVAL_SEC: RT_ARRIVAL_SEC_NAME,
    STOP_SEQUENCE: STOP_SEQUENCE_NAME,
    TRIP_INSTANCE_KEY: TRIP_INSTANCE_KEY_NAME,
    TRIP_ID: TRIP_ID_NAME,
    STOP_ID: STOP_ID_NAME,
    SCHEDULE_GTFS_DATASET_KEY: SCHEDULE_GTFS_DATASET_KEY_NAME,
    RT_ARRIVAL_SEC: RT_ARRIVAL_SEC_NAME,
}

# Kwargs to easily pass to tests
# For most tests
DEFAULT_TEST_FEED_GENERATION_KWARGS = {
    "stop_times_desired_columns": TEST_STOP_TIMES_DESIRED_COLUMNS_DEFAULT,
    "stop_times_table_columns": TEST_DEFAULT_COLUMN_MAP,
}
# For tests checking that non-default columns are present
EXTRA_COLUMNS_FEED_GENERATION_KWARGS = {
    **DEFAULT_TEST_FEED_GENERATION_KWARGS,
    "stop_times_desired_columns": TEST_STOP_TIMES_DESIRED_COLUMNS_EXTRA_COLUMNS,
}
