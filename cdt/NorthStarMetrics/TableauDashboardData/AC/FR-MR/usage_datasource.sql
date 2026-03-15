-- =============================================================================
-- FR-MR USAGE DATASOURCE
-- =============================================================================
-- Purpose: Powers Foundation and Migration Usage % charts (4.2 and 4.4)
-- Grain: One row per deployment (ensures accurate Usage % calculation)
--
-- Updates:
--   - Added biweekly_period column (semi-monthly grain) derived from deployment_start_date
--   - Foundation rows: filtered to last 6 months (based on deployment_start_date)
--   - Migration rows: NO changes (keeps full date range, uses shifted_deployment_date)
--
-- Charts Supported:
--   - 4.2 Foundation Recipe Usage %: AGG(Usage %) by biweekly_period (6 months)
--   - 4.4 Migration Recipe Usage %: AGG(Usage %) by QUARTER(shifted_deployment_date) (unchanged)
--
-- Join Path (matches old "Deployment with Tenant Build" datasource):
--   sfdc_deployments -> sfdc_account_tenant_map -> tenant_build
--   sfdc_deployments -> sfdc_account_details (for filter columns)
--
-- Key Features:
--   - Deployment-grain ensures each deployment counted exactly once
--   - used_recipe_flag = 1 when deployment's account has matching tenant_build event
--   - Column naming aligned with CT-TC datasources (deployment_* prefix)
--   - customer_sf_account_id included for Workday GO join capability
--   - Foundation: 6-month semi-monthly window
--   - Migration: Full date range with shifted_deployment_date (unchanged from v1)
--
-- Filter Columns Available:
--   - enterprise_size_group, segment, super_industry, account_name (from sfdc_account_details)
--   - deployment_product_area, deployment_partner, deployment_phase, deployment_type (from sfdc_deployments)
--   - recipe_type (Foundation/Migration)
--
-- Usage % Calculation in Tableau:
--   COUNTD(IF [used_recipe_flag] = 1 THEN [sf_deployment_id] END) / COUNTD([sf_deployment_id])
-- =============================================================================

-- =============================================================================
-- DATE RANGE CTE: Calculate 6-month semi-monthly window (for Foundation only)
-- =============================================================================
WITH date_range AS (
    SELECT 
        -- Start date: 6 months ago, beginning of that month
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS start_date,
        -- End date: Current semi-monthly period (exclusive)
        -- If day <= 15: current period started on 1st, so end_date = 1st of month
        -- If day > 15: current period started on 16th, so end_date = 16th of month
        CASE 
            WHEN DAY(CURRENT_DATE) <= 15 
                THEN DATE_TRUNC('month', CURRENT_DATE)
            ELSE DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE))
        END AS end_date
),

-- =============================================================================
-- PART 1: FOUNDATION RECIPE USAGE (6-month filter applied)
-- =============================================================================
-- Based on: Old Tableau Data/deployment_with_tenant_build.sql
-- Uses sfdc_account_tenant_map + swh_raw.tenant_build to match OLD behavior exactly
-- =============================================================================

foundation_recipe_deployments AS (
    SELECT DISTINCT 
        d.sf_deployment_id,
        d.customer_sf_account_id,
        d.type,
        d.deployment_start_date,
        DATE_TRUNC('month', CAST(d.deployment_start_date AS DATE)) AS deployment_month,
        -- Semi-monthly period (biweekly_period) derived from deployment_start_date
        CASE 
            WHEN DAY(CAST(d.deployment_start_date AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(d.deployment_start_date AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(d.deployment_start_date AS DATE))))
        END AS biweekly_period,
        -- Deployment filter columns (aligned naming)
        COALESCE(d.product_area, 'Unknown') AS deployment_product_area,
        COALESCE(d.priming_partner_name, 'Unknown') AS deployment_partner,
        COALESCE(d.phase, 'Unknown') AS deployment_phase
    FROM dw.lookup_db.sfdc_deployments d
    CROSS JOIN date_range dr
    WHERE d.overall_status = 'Active'
      AND (d.function_production_move_date_actual IS NULL 
           OR CAST(d.function_production_move_date_actual AS DATE) > DATE '2025-01-01')
      AND d.phase NOT IN ('Adhoc', 'Customer Enablement')
      AND CAST(d.deployment_start_date AS DATE) > DATE '2023-01-01'
      -- V2: 6-month filter for Foundation
      AND CAST(d.deployment_start_date AS DATE) >= dr.start_date
      AND CAST(d.deployment_start_date AS DATE) < dr.end_date
),

foundation_recipe_usage AS (
    SELECT DISTINCT 
        d.sf_deployment_id,
        CAST(tb.wd_event_date AS DATE) AS wd_event_date
    FROM foundation_recipe_deployments d
    INNER JOIN dw.lookup_db.sfdc_account_tenant_map atm 
        ON d.customer_sf_account_id = atm.sf_account_id
    INNER JOIN dw.swh_raw.tenant_build tb 
        ON atm.billing_id = tb.customer_billing_id
    WHERE tb.build_type = 'Foundation Tenant Build'
      AND tb.build_status = 'Completed'
      AND CAST(tb.wd_event_date AS DATE) >= DATE '2023-01-01'
),

-- =============================================================================
-- PART 2: MIGRATION RECIPE USAGE (NO date filter change - kept as original)
-- =============================================================================
-- Based on: Old Tableau Data/deployments_with_migration_recipe.sql
-- Note: Migration has additional phase exclusions and uses shifted_deployment_date
-- Note: Migration does NOT have 6-month filter - keeps full date range
-- =============================================================================

migration_recipe_deployments AS (
    SELECT DISTINCT 
        d.sf_deployment_id,
        d.customer_sf_account_id,
        d.type,
        d.deployment_start_date,
        DATE_TRUNC('month', CAST(d.deployment_start_date AS DATE)) AS deployment_month,
        -- Semi-monthly period (biweekly_period) - included for consistency but not used for Migration chart
        CASE 
            WHEN DAY(CAST(d.deployment_start_date AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(d.deployment_start_date AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(d.deployment_start_date AS DATE))))
        END AS biweekly_period,
        -- Shifted date for Migration Usage % chart (6 months forward) - UNCHANGED
        DATE_ADD('month', 6, CAST(d.deployment_start_date AS DATE)) AS shifted_deployment_date,
        -- Deployment filter columns (aligned naming)
        COALESCE(d.product_area, 'Unknown') AS deployment_product_area,
        COALESCE(d.priming_partner_name, 'Unknown') AS deployment_partner,
        COALESCE(d.phase, 'Unknown') AS deployment_phase
    FROM dw.lookup_db.sfdc_deployments d
    WHERE d.overall_status = 'Active'
      AND (d.function_production_move_date_actual IS NULL 
           OR CAST(d.function_production_move_date_actual AS DATE) > DATE '2025-01-01')
      -- Migration has additional phase exclusions
      AND d.phase NOT IN (
          'Adhoc', 'Customer Enablement', 'Phase X - Sourcing', 
          'Customer Led', 'Peakon First', 'Sourcing First', 
          'Phase X - Peakon', 'Phase X - VNDLY', 'Phase X - Planning'
      )
      AND CAST(d.deployment_start_date AS DATE) > DATE '2023-01-01' 
      -- NO 6-month filter for Migration - keeps full date range
),

migration_recipe_usage AS (
    SELECT DISTINCT 
        d.sf_deployment_id,
        CAST(tb.wd_event_date AS DATE) AS wd_event_date
    FROM migration_recipe_deployments d
    INNER JOIN dw.lookup_db.sfdc_account_tenant_map atm 
        ON d.customer_sf_account_id = atm.sf_account_id
    INNER JOIN dw.swh_raw.tenant_build tb 
        ON atm.billing_id = tb.customer_billing_id
    WHERE tb.build_type = 'Migration Recipe'
      AND tb.build_status = 'Completed'
      AND CAST(tb.wd_event_date AS DATE) >= DATE '2023-01-01'
)

-- =============================================================================
-- FINAL OUTPUT: Combined Foundation + Migration at Deployment Grain
-- =============================================================================

-- Foundation Recipe Deployments (6-month semi-monthly window)
SELECT 
    'Foundation' AS recipe_type,
    frd.sf_deployment_id,
    frd.customer_sf_account_id,
    frd.deployment_month,
    frd.biweekly_period,
    frd.deployment_month AS shifted_deployment_date,  -- Same as deployment_month for Foundation
    frd.deployment_product_area,
    frd.deployment_partner,
    frd.deployment_phase,
    COALESCE(frd.type, 'Unknown') AS deployment_type,
    -- Account filter columns
    COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sad.segment, 'Unknown') AS segment,
    COALESCE(sad.super_industry, 'Unknown') AS super_industry,
    COALESCE(sad.account_name, 'Unknown') AS account_name, -- ADDED HERE
    -- Usage calculation fields
    COALESCE(fru.wd_event_date, CAST(frd.deployment_start_date AS DATE)) AS safe_event_date,
    CASE WHEN fru.sf_deployment_id IS NOT NULL THEN 1 ELSE 0 END AS used_recipe_flag
FROM foundation_recipe_deployments frd
LEFT JOIN foundation_recipe_usage fru 
    ON frd.sf_deployment_id = fru.sf_deployment_id
LEFT JOIN dw.lookup_db.sfdc_account_details sad 
    ON frd.customer_sf_account_id = sad.sf_account_id

UNION ALL

-- Migration Recipe Deployments (full date range, uses shifted_deployment_date)
SELECT 
    'Migration' AS recipe_type,
    mrd.sf_deployment_id,
    mrd.customer_sf_account_id,
    mrd.deployment_month,
    mrd.biweekly_period,  -- Included for consistency but Migration chart uses shifted_deployment_date
    mrd.shifted_deployment_date,  -- 6 months forward for Migration - UNCHANGED
    mrd.deployment_product_area,
    mrd.deployment_partner,
    mrd.deployment_phase,
    COALESCE(mrd.type, 'Unknown') AS deployment_type,
    -- Account filter columns
    COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sad.segment, 'Unknown') AS segment,
    COALESCE(sad.super_industry, 'Unknown') AS super_industry,
    COALESCE(sad.account_name, 'Unknown') AS account_name, -- ADDED HERE
    -- Usage calculation fields
    COALESCE(mru.wd_event_date, CAST(mrd.deployment_start_date AS DATE)) AS safe_event_date,
    CASE WHEN mru.sf_deployment_id IS NOT NULL THEN 1 ELSE 0 END AS used_recipe_flag
FROM migration_recipe_deployments mrd
LEFT JOIN migration_recipe_usage mru 
    ON mrd.sf_deployment_id = mru.sf_deployment_id
LEFT JOIN dw.lookup_db.sfdc_account_details sad 
    ON mrd.customer_sf_account_id = sad.sf_account_id


-- =============================================================================
-- TABLEAU CALCULATED FIELDS
-- =============================================================================
-- 
-- Usage Percentage:
--   COUNTD(IF [used_recipe_flag] = 1 THEN [sf_deployment_id] END) 
--   / 
--   COUNTD([sf_deployment_id])
--
-- Deployment Type Filter (LOD for Initial vs Subsequent):
--   { FIXED [customer_sf_account_id], [biweekly_period] :
--     MAX(IF [deployment_type] != 'Initial Deployment' 
--         THEN 'Subsequent Deployment' 
--         ELSE 'Initial Deployment' END)
--   }
--
-- Is Workday Go Customer? (after Redshift join):
--   NOT ISNULL([accountid])
--
-- =============================================================================


-- =============================================================================
-- TABLEAU USAGE NOTES
-- =============================================================================
-- 
-- Chart 4.2 - Foundation Recipe Usage %:
--   - Filter: [recipe_type] = 'Foundation'
--   - X-axis: [biweekly_period] (new semi-monthly column)
--   - Measure: Usage Percentage calculated field
--
-- Chart 4.4 - Migration Recipe Usage %:
--   - Filter: [recipe_type] = 'Migration'
--   - X-axis: [shifted_deployment_date] (unchanged from v1)
--   - Measure: Usage Percentage calculated field
--
-- =============================================================================


-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================
-- Run these in Trino to verify counts:
--
-- Foundation Usage % by biweekly_period:
-- SELECT 
--     biweekly_period,
--     COUNT(DISTINCT sf_deployment_id) AS total_deployments,
--     COUNT(DISTINCT CASE WHEN used_recipe_flag = 1 THEN sf_deployment_id END) AS used_deployments,
--     CAST(COUNT(DISTINCT CASE WHEN used_recipe_flag = 1 THEN sf_deployment_id END) AS DOUBLE) 
--         / COUNT(DISTINCT sf_deployment_id) AS usage_percentage
-- FROM (<this_query>)
-- WHERE recipe_type = 'Foundation'
-- GROUP BY biweekly_period
-- ORDER BY biweekly_period;
--
-- Migration Usage % (unchanged - full date range):
-- SELECT 
--     DATE_TRUNC('quarter', shifted_deployment_date) AS quarter,
--     COUNT(DISTINCT sf_deployment_id) AS total_deployments,
--     COUNT(DISTINCT CASE WHEN used_recipe_flag = 1 THEN sf_deployment_id END) AS used_deployments,
--     CAST(COUNT(DISTINCT CASE WHEN used_recipe_flag = 1 THEN sf_deployment_id END) AS DOUBLE) 
--         / COUNT(DISTINCT sf_deployment_id) AS usage_percentage
-- FROM (<this_query>)
-- WHERE recipe_type = 'Migration'
-- GROUP BY DATE_TRUNC('quarter', shifted_deployment_date)
-- ORDER BY quarter;
--
-- Verify filter columns have data:
-- SELECT 
--     recipe_type,
--     COUNT(DISTINCT enterprise_size_group) as esg_values,
--     COUNT(DISTINCT segment) as segment_values,
--     COUNT(DISTINCT deployment_product_area) as product_area_values,
--     COUNT(DISTINCT deployment_partner) as partner_values
-- FROM (<this_query>)
-- GROUP BY recipe_type;
--
-- =============================================================================
