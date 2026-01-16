"""
Track a set of curated GTFS tables
that we typically would export for schedule + RT analysis.
These should tables join cleanly,
making use of `trip_instance_key` and other keys we have,
and get them to the desired high-level metrics for
their desired granularity more quickly (only subset of routes for these operators).

Daily tables for now.
Maybe in the future we move them to monthly.
"""

GTFS_MART_TABLES = [
    "fct_scheduled_trips",  # clustered by service_date
    "fct_daily_scheduled_stops",  # partitioned by service_date, clustered by feed_key
    "fct_daily_scheduled_shapes",  # partitioned by service_date
    "fct_observed_trips",  # partitioned by service_date; clustered by service_date, schedule_base64_url
    "fct_vehicle_locations",  # partitioned by dt; clustered by dt, base64_url
    "fct_vehicle_locations_path",  # partitioned by service_date; clustered by base64_url, schedule_base64_url
]

HERMOSA_BEACH_DATES = ["2025-10-15", "2025-11-12", "2025-12-17", "2026-01-14"]

SUBSET_OF_OPERATORS = [
    "Beach Cities GMV Schedule",
    "Beach Cities VehiclePositions",
    "Torrance Schedule",
    "Torrance Vehicle Positions",
    "LA DOT Schedule",
    "LA DOT VehiclePositions",
    "LA Metro Bus Schedule",
    "LA Metro Bus Vehicle Positions",
    "LA Metro Rail Schedule",
    "LA Metro Rail Vehicle Positions",
]
