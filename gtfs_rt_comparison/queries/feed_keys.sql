-- Schedule feed keys for the target feed, and the trip updates feed that goes
-- with each one. Left join because a schedule feed can exist on a date with no
-- trip updates feed at all - those rows are the dates to drop later.
select
    tu.base64_url as tu_base64_url,
    sched.feed_key as schedule_feed_key,
    sched.date
from mart_gtfs.fct_daily_schedule_feeds as sched
left join mart_gtfs.fct_daily_rt_feed_files as tu
    on sched.feed_key = tu.schedule_feed_key
    and sched.date = tu.date
    and tu.feed_type = 'trip_updates'
where
    sched.date in {{ DATES | sql_in }}
    and sched.gtfs_dataset_name = '{{ GTFS_DATASET_NAME }}'
