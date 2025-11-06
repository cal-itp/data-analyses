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
      DATE = (
        SELECT
          DATE
        FROM
          `cal-itp-data-infra.staging.int_gtfs_quality__daily_assessment_candidate_entities`
        ORDER BY
          DATE DESC
        LIMIT
          1
      )
      AND gtfs_dataset_type = 'schedule'
  ),
  -- from airtable grab holiday service information published on websites
  holiday_info AS (
    SELECT
      id AS service_source_record_id,
      name AS service_name,
      holiday_schedule___veterans_day AS hs_vets_day,
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
      AND dt = DATE_SUB(current_date("America/Los_Angeles"), INTERVAL 1 DAY)
      -- filter only services that has current holiday service information published on websites
      AND holiday_website_condition IN ('Current - Implicit Dates', 'Current - Explicit Dates')
  ),
  -- get service info
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
      ss.service_date IN (
        '2025-11-11',
        '2025-11-15', -- Sat
        '2025-11-16', --Sun
        '2025-11-19', -- Wed the week before thanksgiving
        '2025-11-27',
        '2025-11-28'
      )
  ),
  -- get trip info
  add_trips AS (
    SELECT
      pred.*,
      dtl.trip_id
    FROM
      pred
      LEFT JOIN `cal-itp-data-infra.mart_gtfs_schedule_latest.dim_trips_latest` dtl ON pred.feed_key = dtl.feed_key
      AND pred.service_id = dtl.service_id
  ),
  -- calculate number of trips for selected service dates
  grouped_trips AS (
    SELECT
      feed_key,
      feed_publisher_name,
      base64_url,
      feed_start_date,
      feed_end_date,
      SUM(CASE WHEN service_date = '2025-11-11' THEN 1 END) AS _2025_11_11,
      SUM(CASE WHEN service_date = '2025-11-15' THEN 1 END) AS _2025_11_15,
      SUM(CASE WHEN service_date = '2025-11-16' THEN 1 END) AS _2025_11_16,
      SUM(CASE WHEN service_date = '2025-11-19' THEN 1 END) AS _2025_11_19,
      SUM(CASE WHEN service_date = '2025-11-27' THEN 1 END) AS _2025_11_27,
      SUM(CASE WHEN service_date = '2025-11-28' THEN 1 END) AS _2025_11_28
    FROM
      add_trips
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ),
  -- set reference regular service and reduced service, calculate the ratio of holiday service to regular service
  analysis_base AS (
    SELECT
      holiday_info.service_source_record_id,
      holiday_info.service_name,
      holiday_info.hs_vets_day,
      holiday_info.hs_thanksgiving,
      holiday_info.hs_day_after_thanksgiving,
      holiday_info.hs_xmas_eve,
      holiday_info.hs_xmas,
      holiday_info.hs_nye,
      holiday_info.hs_new_years_day,
      holiday_info.holiday_website_condition,
      holiday_info.holiday_schedule_notes,
      the_bridge.organization_name,
      the_bridge.gtfs_dataset_name,
      the_bridge.public_customer_facing_or_regional_subfeed_fixed_route,
      the_bridge.organization_hubspot_company_record_id,
      the_bridge.gtfs_service_data_customer_facing,
      the_bridge.regional_feed_type,
      the_bridge.use_subfeed_for_reports,
      the_bridge.gtfs_dataset_key,
      the_bridge.schedule_feed_key,
      grouped_trips.feed_key,
      grouped_trips.feed_publisher_name,
      grouped_trips.base64_url,
      grouped_trips.feed_start_date,
      grouped_trips.feed_end_date,
      grouped_trips._2025_11_11,
      grouped_trips._2025_11_15, -- Sat
      grouped_trips._2025_11_16, -- Sun
      grouped_trips._2025_11_19, -- Wed
      grouped_trips._2025_11_27,
      grouped_trips._2025_11_28,


      CASE WHEN grouped_trips._2025_11_16 IS NOT NULL AND grouped_trips._2025_11_15 IS NOT NULL THEN LEAST(grouped_trips._2025_11_16, grouped_trips._2025_11_15)
           WHEN grouped_trips._2025_11_16 IS NULL AND grouped_trips._2025_11_15 IS NULL THEN 0
           ELSE COALESCE(grouped_trips._2025_11_16, grouped_trips._2025_11_15) END AS reduced_ref, -- Sat/Sun trips as reduced service reference
      grouped_trips._2025_11_19 as regular_ref, -- Wed trips as regular service reference
      LEAST(1.0*COALESCE(COALESCE(grouped_trips._2025_11_16, grouped_trips._2025_11_15), 0)/grouped_trips._2025_11_19, 1) as red_ratio, -- expected ratio of reduced to regular service
      CASE WHEN 1.0*COALESCE(COALESCE(grouped_trips._2025_11_16, grouped_trips._2025_11_15), 0)/grouped_trips._2025_11_19 = 1 THEN 0
           WHEN 1.0*COALESCE(COALESCE(grouped_trips._2025_11_16, grouped_trips._2025_11_15), 0)/grouped_trips._2025_11_19 >= 0.6 THEN 0.15
           WHEN 1.0*COALESCE(COALESCE(grouped_trips._2025_11_16, grouped_trips._2025_11_15), 0)/grouped_trips._2025_11_19 >= 0.4 THEN 0.3
           ELSE 0.5 END AS tolerance, -- add a buffer when comparing the actual holiday service ratio to expected ratio. This buffer can be adjusted.

      LEAST(1.0*COALESCE(_2025_11_11,0)/_2025_11_19, 1) as vets_ratio, -- actual ratio of veterans day service to regular service
      LEAST(1.0*COALESCE(_2025_11_27,0)/_2025_11_19, 1) as thanksgiving_ratio, -- actual ratio of thanksgiving day service to regular service
      LEAST(1.0*COALESCE(_2025_11_28,0)/_2025_11_19, 1) as thanksgiving_fri_ratio -- actual ratio of thanksgiving Friday service to regular service

    FROM
      holiday_info
      LEFT JOIN the_bridge ON holiday_info.service_source_record_id = the_bridge.service_source_record_id
      LEFT JOIN grouped_trips ON the_bridge.schedule_feed_key = grouped_trips.feed_key
   -- Exclude the regional, aggregated feed from results
   WHERE gtfs_dataset_name != 'Bay Area 511 Regional Schedule'
  ),
-- add GTFS service level label
full_results as (
  SELECT
  organization_name,
  service_name,
  gtfs_dataset_name,
  holiday_website_condition,

  _2025_11_11,
  hs_vets_day,
  vets_ratio,

  CASE WHEN _2025_11_19 IS NULL THEN NULL
       -- when there is no differentiation between reduced and regular service, or no info about reduced service, use fixed threshold inherited from last year
       WHEN red_ratio = 1 AND vets_ratio >= 0.85 THEN 'Regular service'
       WHEN red_ratio = 1 AND vets_ratio <= 0.2 THEN 'No service'
       WHEN red_ratio = 1 AND vets_ratio > 0.2 AND thanksgiving_ratio < 0.85 THEN 'Reduced service'
       WHEN red_ratio = 0 AND vets_ratio >= 0.85 THEN 'Regular service'
       WHEN red_ratio = 0 AND vets_ratio <= 0.2 THEN 'No service'
       WHEN red_ratio = 0 AND vets_ratio > 0.2 AND thanksgiving_ratio < 0.85 THEN 'Reduced service'
       -- keep the fixed threshold, and also refer to the ratio of reduced service to regular service if we have a reference
       WHEN vets_ratio >= 1 THEN 'Regular service'
       WHEN vets_ratio <= LEAST(0.2, red_ratio*(1-tolerance)) THEN 'No service'
       WHEN vets_ratio > LEAST(0.2, red_ratio*(1-tolerance)) AND vets_ratio < GREATEST(0.85, red_ratio*(1+tolerance)) THEN 'Reduced service'
       WHEN vets_ratio >= GREATEST(0.85, red_ratio*(1+tolerance)) THEN 'Regular service'
       ELSE 'Uncertain' END AS gtfs_veterans,

  _2025_11_15,
  _2025_11_16,
  _2025_11_19,

  reduced_ref,
  regular_ref,
  red_ratio,

  _2025_11_27,
  hs_thanksgiving,
  thanksgiving_ratio,
  CASE WHEN _2025_11_19 IS NULL THEN NULL
       WHEN red_ratio = 1 AND thanksgiving_ratio >= 0.85 THEN 'Regular service'
       WHEN red_ratio = 1 AND thanksgiving_ratio <= 0.2 THEN 'No service'
       WHEN red_ratio = 1 AND thanksgiving_ratio > 0.2 AND thanksgiving_ratio < 0.85 THEN 'Reduced service'
       WHEN red_ratio = 0 AND thanksgiving_ratio >= 0.85 THEN 'Regular service'
       WHEN red_ratio = 0 AND thanksgiving_ratio <= 0.2 THEN 'No service'
       WHEN red_ratio = 0 AND thanksgiving_ratio > 0.2 AND thanksgiving_ratio < 0.85 THEN 'Reduced service'
       WHEN thanksgiving_ratio >= 1 THEN 'Regular service'
       WHEN thanksgiving_ratio <= LEAST(0.2, red_ratio*(1-tolerance)) THEN 'No service'
       WHEN thanksgiving_ratio > LEAST(0.2, red_ratio*(1-tolerance)) AND thanksgiving_ratio < GREATEST(0.85, red_ratio*(1+tolerance)) THEN 'Reduced service'
       WHEN thanksgiving_ratio >= GREATEST(0.85, red_ratio*(1+tolerance)) THEN 'Regular service'
       ELSE 'Uncertain' END AS gtfs_thanksgiving,

  _2025_11_28,
  hs_day_after_thanksgiving,
  thanksgiving_fri_ratio,
  CASE WHEN _2025_11_19 IS NULL THEN NULL
       WHEN red_ratio = 1 AND thanksgiving_fri_ratio >= 0.85 THEN 'Regular service'
       WHEN red_ratio = 1 AND thanksgiving_fri_ratio <= 0.2 THEN 'No service'
       WHEN red_ratio = 1 AND thanksgiving_fri_ratio > 0.2 AND thanksgiving_ratio < 0.85 THEN 'Reduced service'
       WHEN red_ratio = 0 AND thanksgiving_fri_ratio >= 0.85 THEN 'Regular service'
       WHEN red_ratio = 0 AND thanksgiving_fri_ratio <= 0.2 THEN 'No service'
       WHEN red_ratio = 0 AND thanksgiving_fri_ratio > 0.2 AND thanksgiving_ratio < 0.85 THEN 'Reduced service'
       WHEN thanksgiving_fri_ratio >= 1 THEN 'Regular service'
       WHEN thanksgiving_fri_ratio <= LEAST(0.2, red_ratio*(1-tolerance)) THEN 'No service'
       WHEN thanksgiving_fri_ratio > LEAST(0.2, red_ratio*(1-tolerance)) AND thanksgiving_fri_ratio < GREATEST(0.85, red_ratio*(1+tolerance)) THEN 'Reduced service'
       WHEN thanksgiving_fri_ratio >= GREATEST(0.85, red_ratio*(1+tolerance)) THEN 'Regular service'
       ELSE 'Uncertain' END AS gtfs_thanksgiving_fri,

  public_customer_facing_or_regional_subfeed_fixed_route,
  use_subfeed_for_reports,
  CAST(FROM_BASE64(REPLACE(REPLACE(base64_url, '-', '+'), '_', '/')) AS STRING) AS website,
  feed_start_date,
  feed_end_date,
  organization_hubspot_company_record_id,
FROM
  analysis_base
),
-- mismatch label for holidays
output_base as (
  SELECT
      organization_name,
      service_name,
      gtfs_dataset_name,
      holiday_website_condition,

      _2025_11_11,
      hs_vets_day,
      vets_ratio,
      gtfs_veterans,
      CASE WHEN gtfs_veterans IS NULL THEN NULL
          WHEN gtfs_veterans = hs_vets_day THEN 0
          WHEN hs_vets_day = 'Uncertain' THEN 0
          ELSE 1 END AS mismatch_veterans,

      _2025_11_15,
      _2025_11_16,
      _2025_11_19,

      reduced_ref,
      regular_ref,
      red_ratio,

      _2025_11_27,
      hs_thanksgiving,
      thanksgiving_ratio,
      gtfs_thanksgiving,
      CASE WHEN gtfs_thanksgiving IS NULL THEN NULL
          WHEN gtfs_thanksgiving = hs_thanksgiving THEN 0
          WHEN hs_thanksgiving = 'Uncertain' THEN 0
          ELSE 1 END AS mismatch_thanksgiving,

      _2025_11_28,
      hs_day_after_thanksgiving,
      thanksgiving_fri_ratio,
      gtfs_thanksgiving_fri,
      CASE WHEN gtfs_thanksgiving_fri IS NULL THEN NULL
          WHEN gtfs_thanksgiving_fri = hs_day_after_thanksgiving THEN 0
          WHEN hs_day_after_thanksgiving = 'Uncertain' THEN 0
          ELSE 1 END AS mismatch_thanksgiving_fri,

      public_customer_facing_or_regional_subfeed_fixed_route,
      use_subfeed_for_reports,
      website,
      feed_start_date,
      feed_end_date,
      organization_hubspot_company_record_id,
    FROM
      full_results
)
-- rename columns, output and sort results
SELECT
    -- Agency and Service Identifiers
    organization_name as `Organization Name`,
    service_name as `Service Name`,
    gtfs_dataset_name as `GTFS Dataset Name`,
    holiday_website_condition `Holiday Website Condition`,
    
    -- Reference reduced service and regular service trips
    reduced_ref as `Reduced Service Reference`,
    regular_ref as `Regular Service Reference`,
    red_ratio as `Reduced Service % of Regular Service`,
    
    -- veterans day
    _2025_11_11 as `Veterans GTFS Trips`,
    hs_vets_day as `Veterans Website Schedule`,
    vets_ratio as `Veterans % of Regular Service in GTFS`,
    gtfs_veterans as `Veterans GTFS Schedule`,
    mismatch_veterans as `Mismatch Veterans`,

    -- Thanksgiving
    _2025_11_27 as `Thanksgiving GTFS Trips`,
    hs_thanksgiving as `Thanksgiving Website Schedule`,
    thanksgiving_ratio as `Thanksgiving % of Regular Service in GTFS`,
    gtfs_thanksgiving as `Thanksgiving GTFS Schedule`,
    mismatch_thanksgiving as `Mismatch Thanksgiving`,

    -- Day after Thanksgiving
    _2025_11_28 as `Thanksgiving Friday GTFS Trips`,
    hs_day_after_thanksgiving as `Thanksgiving Friday Website Schedule`,
    thanksgiving_fri_ratio as `Thanksgiving Friday % of Regular Service in GTFS`,
    gtfs_thanksgiving_fri as `Thanksgiving Friday GTFS Schedule`,
    mismatch_thanksgiving_fri as `Mismatch Thanksgiving Friday`,

    CASE WHEN mismatch_thanksgiving = 1 THEN 1
         WHEN mismatch_thanksgiving_fri = 1 THEN 1
    END AS `Any Thanksgiving Mismatch`,

    public_customer_facing_or_regional_subfeed_fixed_route as `Public Customer Facing or Regional Subfeed Fixed Route`,
    use_subfeed_for_reports as `Use Subfeed for Reports`,
    website as `Website`,
    feed_start_date as `Feed Start Date`,
    feed_end_date as `Feed Start Date`,
    organization_hubspot_company_record_id as `Organization Hubspot Company Record ID`,
  FROM
    output_base
ORDER BY
  organization_name,
  service_name