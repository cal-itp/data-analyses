-- The _valid_from of each schedule feed version. dim_stop_arrivals is
-- partitioned by _feed_valid_from, so this is what lets that query prune.
select
    key as feed_key,
    _valid_from as feed_valid_from
from mart_gtfs.dim_schedule_feeds
where key in {{ FEED_KEYS | sql_in }}
