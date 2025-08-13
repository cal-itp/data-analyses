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

RT_ARRIVAL_SEC = "rt_arrival_sec"
TRIP_INSTANCE_KEY = "trip_instance_key"
SCHEDULE_ARRIVAL_SEC = "schedule_arrival_sec"
STOP_SEQUENCE = "stop_sequence"
TRIP_ID = "trip_id"
STOP_ID = "stop_id"
SCHEDULE_GTFS_DATASET_KEY = "schedule_gtfs_dataset_key"

# Rename these values if column names change in the schedule/rt dataset
# Scheduled arrival time, in seconds after twelve hours before noon 
SCHEDULE_ARRIVAL_SEC_NAME = "scheduled_arrival_sec"
# RT arrival time, in seconds after twelve hours before noon
RT_ARRIVAL_SEC_NAME = "rt_arrival_sec"
# The stop sequence value
STOP_SEQUENCE_NAME = "stop_sequence"
# The column containing the trip instance key, that uniquely identifies trips, including between different agencies
TRIP_INSTANCE_KEY_NAME = "trip_instance_key"
# The column containing the trip id, which can be used to merge trips from the rt table to the schedule feed
TRIP_ID_NAME = "trip_id"
# The coulmn containing the stop id, which should be consistent between the rt table and the schedule feed
STOP_ID_NAME = "stop_id"
# The schedule gtfs dataset key
SCHEDULE_GTFS_DATASET_KEY_NAME = "schedule_gtfs_dataset_key"


TEST_DEFAULT_COLUMN_MAP = {
    SCHEDULE_ARRIVAL_SEC: SCHEDULE_ARRIVAL_SEC_NAME,
    RT_ARRIVAL_SEC: RT_ARRIVAL_SEC_NAME,
    STOP_SEQUENCE: STOP_SEQUENCE_NAME,
    TRIP_INSTANCE_KEY: TRIP_INSTANCE_KEY_NAME,
    TRIP_ID: TRIP_ID_NAME,
    STOP_ID: STOP_ID_NAME,
    SCHEDULE_GTFS_DATASET_KEY: SCHEDULE_GTFS_DATASET_KEY_NAME,
    RT_ARRIVAL_SEC: "gap_imputed_sec",
}

DEFAULT_TEST_FEED_GENERATION_KWARGS = {
    "stop_times_desired_columns": TEST_STOP_TIMES_DESIRED_COLUMNS_DEFAULT,
    "stop_times_table_columns": TEST_DEFAULT_COLUMN_MAP,
}

EXTRA_COLUMNS_FEED_GENERATION_KWARGS = {
    **DEFAULT_TEST_FEED_GENERATION_KWARGS,
    "stop_times_desired_columns": TEST_STOP_TIMES_DESIRED_COLUMNS_EXTRA_COLUMNS
}