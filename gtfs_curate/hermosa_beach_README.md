# README

## Tables Provided
* fct_scheduled_trips - daily GTFS scheduled trips
* fct_daily_scheduled_stops - daily GTFS scheduled stops
* fct_daily_scheduled_shapes - daily GTFS scheduled shapes
* fct_observed_trips - daily GTFS vehicle positions and trip updates trip-level summaries. Each row is a trip. Columns provide basic summary statistics GTFS RT trip, and can be joined to fct_scheduled_trips using `trip_instance_key`.
* fct_vehicle_locations - daily GTFS deduped vehicle positions, usually every 30 seconds. Each row is a trip-vehicle_id-GPS coordinate-timestamp.
* fct_vehicle_locations_path - daily GTFS deduped vehicle positions, usually every 30 seconds. fct_vehicle_locations is presented as trip-level, and GPS coordinates are stored as an array. This is the wide format of fct_vehicle_locations, usually used for visualizing the entire vehicle positions path by trip.

## Dates Provided
* 2025-10-15
* 2025-11-12
* 2025-12-17
* 2026-01-14

## Useful sites
* analysis.dds.dot.ca.gov
* https://dbt-docs.dds.dot.ca.gov/index.html#!/overview

## Data dictionary
* dbt docs - our warehouse column definitions.
* You can type in search bar to find a particular table, such as searching `fct_scheduled_trips` will provide column definitions for that table.
