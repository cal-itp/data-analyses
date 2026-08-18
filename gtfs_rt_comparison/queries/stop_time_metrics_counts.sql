-- Count fct_stop_time_metrics rows per trip updates feed per date. The dates
-- with a significant number of rows are the dates where both TU and VP based
-- stop times are available.
select
    base64_url as tu_base64_url,
    service_date,
    count(*) as n_stop_time_metrics
from mart_gtfs.fct_stop_time_metrics
where
    service_date in {{ DATES | sql_in }}
    and base64_url in {{ TU_URLS | sql_in }}
group by base64_url, service_date
order by service_date
