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

# Column names in input data
SCHEDULE_ARRIVAL_SEC_NAME = "scheduled_arrival_sec"
RT_ARRIVAL_SEC_NAME = "rt_arrival_sec"
STOP_SEQUENCE_NAME = "stop_sequence"
TRIP_INSTANCE_KEY_NAME = "trip_instance_key"
TRIP_ID_NAME = "trip_id"
STOP_ID_NAME = "stop_id"
SCHEDULE_GTFS_DATASET_KEY_NAME = "schedule_gtfs_dataset_key"

# Kwargs to easily pass to tests
# For most tests
DEFAULT_TEST_FEED_GENERATION_KWARGS = {
    "stop_times_desired_columns": TEST_STOP_TIMES_DESIRED_COLUMNS_DEFAULT,
}
# For tests checking that non-default columns are present
EXTRA_COLUMNS_FEED_GENERATION_KWARGS = {
    "stop_times_desired_columns": TEST_STOP_TIMES_DESIRED_COLUMNS_EXTRA_COLUMNS,
}
