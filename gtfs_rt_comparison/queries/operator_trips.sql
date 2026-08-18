-- Every scheduled trip this operator ran on the dates of interest. The VP
-- parquets are statewide, so these keys are what filters them down; keeping all
-- of them also keeps trips that VP saw but TU never predicted.
select trip_instance_key, trip_id, service_date
from mart_gtfs.fct_scheduled_trips
where
    service_date in {{ DATES | sql_in }}
    and name = '{{ GTFS_DATASET_NAME }}'
