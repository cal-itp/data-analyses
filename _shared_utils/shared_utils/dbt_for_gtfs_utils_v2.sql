/*
Move gtfs_utils_v2 into SQl to sit as dbt model to create another
mart_* table that analysts start with.

-- NEW TABLES TO ADD (rename everything if necessary) --
1. fact_feeds (rename to something else?)
    Purpose: see what feed_keys - organization names are present on selected day
    Use this to query or subset against if analysts want to subset by
    organization name, as opposed to feed_key

2. mart_trips
    Purpose: be the primary trips query for analysts to start
    Combines: `fct_daily_scheduled_trips` + `fact_feeds` + `dim_trips` + `dim_routes`

    Replace: gtfs_utils
    v2: `gtfs_utils_v2.get_trips`
    v1: `gtfs_utils.get_trips` + `gtfs_utils.get_route_info`

3. mart_shapes
    Purpose: shapes present on day
    Replace: gtfs_utils_v2.get_route_shapes, gtfs_utils.get_route_shapes

4. mart_stops
    Purpose: make point geom for stops present on day
    Replace: gtfs_utils_v2.get_stops, gtfs_utils.get_stops

*/


/* DAILY FEED KEY TO ORGANIZATION
-- Can we add this to every table for get_trips, get_routes, ...get_stops
-- so that we can subset by name?
*/
WITH dim_gtfs_datasets AS (
    SELECT *
    FROM {{ ref('dim_gtfs_datasets') }}
),

fct_daily_schedule_feeds AS (
    SELECT *
    FROM {{ ref('fct_daily_schedule_feeds') }}
),

fact_feeds AS (
    SELECT
        dim_gtfs_datasets.name,
        dim_gtfs_datasets.regional_feed_type,

        schedule_feeds.feed_key,
        schedule_feeds.date as service_date,
        schedule_feeds.feed_key,
        schedule_feeds.base64_url,
        schedule_feeds.gtfs_dataset_key,

    FROM dim_gtfs_datasets
        WHERE dim_gtfs_datasets.data = 'GTFS Schedule'
    INNER JOIN fct_daily_schedule_feeds AS schedule_feeds
        WHERE schedule_feeds.is_future is False
        ON schedule_feeds.gtfs_dataset_key = dim_gtfs_datasets.key
)

SELECT * FROM fact_feeds


/* MART_TRIPS

expanded version, add in dim_routes, broadly useful for analysts
to subset using route_type or route_name
*/
WITH fct_daily_scheduled_trips AS (
    SELECT *
    FROM {{ ref('fct_daily_scheduled_trips') }}
),

dim_trips AS (
    SELECT *
    FROM {{ ref('dim_trips') }}
),

-- This table would need to be created
fact_feeds AS (
    SELECT *
    FROM {{ ref('fact_feeds') }}
),

mart_trips AS (
    SELECT
        fct_trips.trip_key,
        fct_trips.service_date,
        fct_trips.feed_key,
        fct_trips.service_id,
        fct_trips.trip_id,
        fct_trips.route_key,
        fct_trips.shape_array_key,
        fct_trips.gtfs_dataset_key,
        fct_trips.contains_warning_duplicate_trip_primary_key,
        fct_trips.n_stops,
        fct_trips.n_stop_times,
        fct_trips.trip_first_departure_sec,
        fct_trips.trip_last_arrival_sec,
        fct_trips.service_hours,
        fct_trips.contains_warning_duplicate_stop_times_primary_key,
        fct_trips.contains_warning_missing_foreign_key_stop_id,

        dim_trips.route_id,
        dim_trips.trip_short_name,
        dim_trips.direction_id,
        dim_trips.block_id,

        dim_routes.route_type,
        dim_routes.route_short_name,
        dim_routes.route_long_name,
        dim_routes.route_desc,

        fact_feeds.name,
        fact_feeds.regional_feed_type,

    FROM fct_daily_scheduled_trips
    INNER JOIN dim_trips
        ON fct_daily_scheduled_trips.trip_key = dim_trips.key
    LEFT JOIN dim_routes
        ON fct_daily_scheduled_trips.route_key = dim_routes.key
    LEFT JOIN fact_feeds
        ON fact_feeds.gtfs_dataset_key = fct_trips.feed_key
    -- is this last left join correct?
)

SELECT * FROM mart_trips


/*
MART_SHAPES

Add a column that counts how many trips occurred for that shape_id
Might be useful way to sort later, since analysts either select 1 shape_id
for the route based on number of trips or length
*/
WITH fct_daily_scheduled_trips AS (
    SELECT *
    FROM {{ ref('fct_daily_scheduled_trips') }}
),


agg_trips AS (
    SELECT
        service_date,
        feed_key,
        shape_array_key,
        shape_id,
        COUNT(DISTINCT trip_key) AS n_trips,

    FROM fct_daily_scheduled_trips
    GROUP BY service_date, shape_array_key
),

dim_shapes_arrays AS (
    SELECT *
    FROM {{ ref('dim_shapes_arrays') }}
),

mart_shapes AS (
    SELECT
        trips.feed_key,
        trips.shape_array_key,
        trips.shape_id,

        dim_shapes_arrays.pt_array,

    FROM agg_trips AS trips
    INNER JOIN dim_shapes_arrays
        ON trips.shape_array_key = dim_shapes_arrays.key
)

SELECT * FROM mart_shapes

/*
MART_STOPS

pre-assemble with point geom in dbt, always WGS84
TODO: also work in route_type from dim_routes?
*/
WITH fct_daily_scheduled_trips AS (
    SELECT DISTINCT
        stop_key,
        route_key,
        service_date
    FROM {{ ref('fct_daily_scheduled_trips') }}
),

dim_stops AS (
    SELECT *
    FROM {{ ref('dim_stops') }}
),

dim_routes AS (
    SELECT DISTINCT
        key,
        route_type,
    FROM {{ ref('dim_routes') }}
),

stops_geom AS (
    SELECT
        key,
        feed_key,
        stop_id,
        tts_stop_name,
        ST_GEOGPOINT(
            stop_lon,
            stop_lat
        ) AS pt_geom,
        parent_station,
        stop_code,
        stop_name,
        stop_desc,
        location_type,
        stop_timezone,
        wheelchair_boarding,
    FROM dim_stops
),

mart_stops AS (
    FROM fct_daily_scheduled_trips
    INNER JOIN stops_geom
        ON fct_daily_scheduled_trips.stop_key = stops_geom.key
    LEFT JOIN dim_routes
        ON fct_daily_scheduled_trips.route_key = dim_routes.key
)

SELECT * FROM mart_stops
