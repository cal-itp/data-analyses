-- TU based stop times, keyed the way the VP stop times are (schedule trip_instance_key + service_date + stop_sequence).

-- Ordered by trip and stop so the frame arrives ready for within-trip lags.
with stm as (
    select service_date, schedule_base64_url, trip_id, stop_id, stop_sequence,
           actual_arrival_pacific, n_predictions
    from mart_gtfs.fct_stop_time_metrics
    where
        service_date in {{ DATES | sql_in }}
        and base64_url in {{ TU_URLS | sql_in }}
),
sched as (
    select trip_instance_key, trip_id, base64_url, service_date
    from mart_gtfs.fct_scheduled_trips
    where
        service_date in {{ DATES | sql_in }}
        and name = '{{ GTFS_DATASET_NAME }}'
)
select
    sched.trip_instance_key,
    stm.service_date,
    stm.stop_sequence,
    stm.stop_id,
    stm.actual_arrival_pacific as tu_arrival_time,
    stm.n_predictions
from stm
left join sched
    on sched.service_date = stm.service_date
    and sched.trip_id = stm.trip_id
    and sched.base64_url = stm.schedule_base64_url
order by trip_instance_key, stop_sequence
