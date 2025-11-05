/**
 * This SQL query identifies mismatches between holiday service information provided on transit agency websites
 * and the actual service data in their GTFS feeds for specific holidays in late 2024 and early 2025.
 * It combines data from a GTFS staging table, an external Airtable with holiday schedules, and GTFS trip data.
 * This serves as a baseline for further analysis on holiday service discrepancies.
 */
WITH
  the_bridge AS (
    SELECT
      organization_name,
      service_name,
      gtfs_dataset_name,
      organization_source_record_id,
      service_source_record_id,
      gtfs_dataset_source_record_id,
      public_customer_facing_or_regional_subfeed_fixed_route,
      organization_hubspot_company_record_id,
      gtfs_service_data_customer_facing,
      regional_feed_type,
      use_subfeed_for_reports,
      gtfs_dataset_key,
      schedule_feed_key
    FROM
      `cal-itp-data-infra.staging.int_gtfs_quality__daily_assessment_candidate_entities`
    WHERE
      -- Select data for the most recent date available
      date = (
        SELECT
          date
        FROM
          `cal-itp-data-infra.staging.int_gtfs_quality__daily_assessment_candidate_entities`
        ORDER BY
          date DESC
        LIMIT 1
      )
      AND gtfs_dataset_type = 'schedule'
  ),
  holiday_info AS (
    SELECT
      id AS service_source_record_id,
      name AS service_name,
      holiday_schedule___veterans_day__observed_ AS hs_vets_day_obs,
      holiday_schedule___veterans_day AS hs_vets_day_actual,
      holiday_schedule___thanksgiving_day AS hs_thanksgiving,
      holiday_schedule___day_after_thanksgiving_day AS hs_day_after_thanksgiving,
      holiday_schedule___christmas_eve AS hs_xmas_eve,
      holiday_schedule___christmas_day AS hs_xmas,
      holiday_schedule___new_year_s_eve AS hs_nye,
      holiday_schedule___new_year_s_day AS hs_new_years_day,
      holiday_website_condition,
      holiday_schedule_notes
    FROM
      `cal-itp-data-infra.external_airtable.california_transit__services`
    WHERE
      holiday_website_condition IS NOT NULL
      -- Filter for the current date's Airtable snapshot
      AND dt = EXTRACT(DATE FROM CURRENT_TIMESTAMP() AT TIME ZONE 'America/Los_Angeles')
  ),
  pred AS (
    SELECT
      ss.feed_key,
      feed_publisher_name,
      feed_start_date,
      feed_end_date,
      feed_info.base64_url,
      ss.service_date,
      ss.service_id
    FROM
      `cal-itp-data-infra.mart_gtfs_schedule_latest.dim_feed_info_latest` AS feed_info
      LEFT JOIN `cal-itp-data-infra.staging.int_gtfs_schedule__all_scheduled_service` AS ss ON feed_info.feed_key = ss.feed_key
    WHERE
      -- Filter for specific holiday and surrounding dates
      ss.service_date IN ('2024-11-11', '2024-11-27', '2024-11-28', '2024-11-29', '2024-12-18', '2024-12-23', '2024-12-24', '2024-12-25', '2024-12-31', '2025-01-01')
  ),
  add_trips AS (
    SELECT
      pred.*,
      dtl.trip_id
    FROM
      pred
      LEFT JOIN `cal-itp-data-infra.mart_gtfs_schedule_latest.dim_trips_latest` AS dtl ON pred.feed_key = dtl.feed_key
      AND pred.service_id = dtl.service_id
  ),
  grouped_trips AS (
    SELECT
      feed_key,
      feed_publisher_name,
      base64_url,
      feed_start_date,
      feed_end_date,
      -- Count trips for each specific holiday date
      SUM(CASE WHEN service_date = '2024-11-11' THEN 1 END) AS _2024_11_11,
      SUM(CASE WHEN service_date = '2024-11-27' THEN 1 END) AS _2024_11_27,
      SUM(CASE WHEN service_date = '2024-11-28' THEN 1 END) AS _2024_11_28,
      SUM(CASE WHEN service_date = '2024-11-29' THEN 1 END) AS _2024_11_29,
      SUM(CASE WHEN service_date = '2024-12-18' THEN 1 END) AS _2024_12_18,
      SUM(CASE WHEN service_date = '2024-12-23' THEN 1 END) AS _2024_12_23,
      SUM(CASE WHEN service_date = '2024-12-24' THEN 1 END) AS _2024_12_24,
      SUM(CASE WHEN service_date = '2024-12-25' THEN 1 END) AS _2024_12_25,
      SUM(CASE WHEN service_date = '2024-12-31' THEN 1 END) AS _2024_12_31,
      SUM(CASE WHEN service_date = '2025-01-01' THEN 1 END) AS _2025_01_01
    FROM
      add_trips
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ),
  full_results AS (
    SELECT
      -- Holiday Info Fields (Airtable)
      holiday_info.service_source_record_id,
      holiday_info.service_name,
      holiday_info.hs_vets_day_obs,
      holiday_info.hs_vets_day_actual,
      holiday_info.hs_thanksgiving,
      holiday_info.hs_day_after_thanksgiving,
      holiday_info.hs_xmas_eve,
      holiday_info.hs_xmas,
      holiday_info.hs_nye,
      holiday_info.hs_new_years_day,
      holiday_info.holiday_website_condition,
      holiday_info.holiday_schedule_notes,
      -- Agency/GTFS Metadata Fields (Staging)
      the_bridge.organization_name,
      the_bridge.gtfs_dataset_name,
      the_bridge.public_customer_facing_or_regional_subfeed_fixed_route,
      the_bridge.organization_hubspot_company_record_id,
      the_bridge.gtfs_service_data_customer_facing,
      the_bridge.regional_feed_type,
      the_bridge.use_subfeed_for_reports,
      the_bridge.gtfs_dataset_key,
      the_bridge.schedule_feed_key,
      -- GTFS Trip Counts Fields (Mart)
      grouped_trips.feed_key,
      grouped_trips.feed_publisher_name,
      grouped_trips.base64_url,
      grouped_trips.feed_start_date,
      grouped_trips.feed_end_date,
      grouped_trips._2024_11_11,
      grouped_trips._2024_11_27,
      grouped_trips._2024_11_28,
      grouped_trips._2024_11_29,
      grouped_trips._2024_12_18,
      grouped_trips._2024_12_23,
      grouped_trips._2024_12_24,
      grouped_trips._2024_12_25,
      grouped_trips._2024_12_31,
      grouped_trips._2025_01_01
    FROM
      holiday_info
      LEFT JOIN the_bridge ON holiday_info.service_source_record_id = the_bridge.service_source_record_id
      LEFT JOIN grouped_trips ON the_bridge.schedule_feed_key = grouped_trips.feed_key
  )
SELECT
  -- Agency and Service Identifiers
  organization_name,
  service_name,
  gtfs_dataset_name,
  -- Website Holiday Schedule Information
  holiday_website_condition,
  hs_vets_day_actual,
  hs_thanksgiving,
  hs_day_after_thanksgiving,
  hs_xmas_eve,
  hs_xmas,
  hs_nye,
  hs_new_years_day,
  -- GTFS Trip Counts for Specific Dates
  _2024_11_11,
  _2024_11_27 AS weekday_exp_11_27, -- Explicitly naming the weekday expectation
  _2024_11_28,
  _2024_11_29,
  _2024_12_18 AS weekday_exp_12_18, -- Explicitly naming the weekday expectation
  _2024_12_23 AS weekday_exp_12_23, -- Explicitly naming the weekday expectation
  _2024_12_24,
  _2024_12_25,
  _2024_12_31,
  _2025_01_01,
  -- GTFS Feed Metadata and Context
  public_customer_facing_or_regional_subfeed_fixed_route,
  use_subfeed_for_reports,
  CAST(FROM_BASE64(REPLACE(REPLACE(base64_url, '-', '+'), '_', '/')) AS STRING) AS website, -- Decodes base64 URL for easier access
  feed_start_date,
  feed_end_date,
  organization_hubspot_company_record_id
FROM
  full_results
WHERE
  -- Exclude the regional, aggregated feed from results
  gtfs_dataset_name != 'Bay Area 511 Regional Schedule'
ORDER BY
  organization_name,
  service_name
