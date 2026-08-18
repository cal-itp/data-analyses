-- Scheduled stop times, keyed by trip_instance_key + service_date + stop_sequence.
-- dim_stop_arrivals is per feed version, not per date. fct_scheduled_trips
-- expands a feed's trips out per service date and carries trip_instance_key,
-- which is the key the VP stop arrivals use.
--
-- dim_stop_arrivals is partitioned by _feed_valid_from and clustered by
-- feed_key, so both have to be literals here.
with sched_trips as (
    select trip_instance_key, feed_key, trip_id, service_date
    from mart_gtfs.fct_scheduled_trips
    where
        service_date in {{ DATES | sql_in }}
        and name = '{{ GTFS_DATASET_NAME }}'
),
arrivals as (
    select feed_key, trip_id, stop_id, stop_sequence, feed_timezone,
           arrival_sec, departure_sec
    from mart_gtfs.dim_stop_arrivals
    where
        _feed_valid_from in {{ FEED_VALID_FROMS | sql_in_timestamps }}
        and feed_key in {{ FEED_KEYS | sql_in }}
)
-- arrival_sec/departure_sec are gtfs time - count from midnight and are allowed to run past
-- 24h on owl trips, so they are added to midnight in the feed's own timezone
-- and converted to Pacific, matching how the VP and TU arrival times read.
select
    sched_trips.trip_instance_key,
    sched_trips.service_date,
    arrivals.stop_sequence,
    arrivals.stop_id,
    datetime(
        timestamp_add(
            timestamp(sched_trips.service_date, arrivals.feed_timezone),
            interval arrivals.arrival_sec second
        ),
        'America/Los_Angeles'
    ) as schedule_arrival_time,
    datetime(
        timestamp_add(
            timestamp(sched_trips.service_date, arrivals.feed_timezone),
            interval arrivals.departure_sec second
        ),
        'America/Los_Angeles'
    ) as schedule_departure_time
from arrivals
inner join sched_trips
    on sched_trips.feed_key = arrivals.feed_key
    and sched_trips.trip_id = arrivals.trip_id
order by trip_instance_key, stop_sequence
