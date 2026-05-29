WITH stops AS (
    SELECT
        base64_url,
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        location_type,
        wheelchair_boarding,
        on_state_highway_network
    FROM `cal-itp-data-infra.mart_gtfs_schedule_latest.dim_stops_latest`
),

stop_routes AS (
    SELECT DISTINCT
        st.base64_url,
        st.stop_id,
        r.route_type
    FROM `cal-itp-data-infra.mart_gtfs_schedule_latest.dim_stop_times_latest` st

    JOIN `cal-itp-data-infra.mart_gtfs_schedule_latest.dim_trips_latest` t
        USING (base64_url, trip_id)

    JOIN `cal-itp-data-infra.mart_gtfs_schedule_latest.dim_routes_latest` r
        USING (base64_url, route_id)
),

stops_with_mode AS (
    SELECT
        s.*,
        sr.route_type,

        CASE
            WHEN sr.route_type = '3'
                THEN 'Bus'

            WHEN sr.route_type IN (
                '0',   -- Tram
                '1',   -- Subway
                '2',   -- Rail
                '5',   -- Cable Tram
                '6',   -- Gondola
                '7',   -- Funicular
                '11',  -- Trolleybus
                '12'   -- Monorail
            )
                THEN 'Rail'

            ELSE 'Other'

        END AS mode_type

    FROM stops s

    LEFT JOIN stop_routes sr
        USING(base64_url, stop_id)
),

datasets AS (
    SELECT
        dg.key AS gtfs_dataset_key,
        swm.*

    FROM stops_with_mode swm

    JOIN
    `cal-itp-data-infra.mart_transit_database.dim_gtfs_datasets` dg
        USING(base64_url)

    WHERE dg._is_current = TRUE
),

providers AS (
    SELECT
        d.*,
        dp.organization_name

    FROM datasets d

    JOIN
    `cal-itp-data-infra.mart_transit_database.dim_provider_gtfs_data` dp
        ON d.gtfs_dataset_key =
           dp.schedule_gtfs_dataset_key

    WHERE
        dp._is_current = TRUE
        AND dp.public_customer_facing_or_regional_subfeed_fixed_route
)

SELECT DISTINCT
    organization_name,
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    location_type,
    wheelchair_boarding,
    on_state_highway_network,
    route_type,
    mode_type

FROM providers
