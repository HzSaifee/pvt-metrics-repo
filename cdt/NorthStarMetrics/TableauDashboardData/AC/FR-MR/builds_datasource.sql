-- =============================================================================
-- FR-MR BUILDS DATASOURCE
-- =============================================================================
-- Purpose: Powers Foundation and Migration Builds charts (4.1 and 4.3)
-- Grain: One row per tenant_build event (may have multiple deployments per account)
--
-- Updates:
--   - Rolling window changed from 12 months to 6 months
--   - Added biweekly_period column (semi-monthly grain) derived from wd_event_date
--   - Semi-monthly logic: Days 1-15 → 1st of month, Days 16-31 → 16th of month
--   - Deployment scope: 6mo Active (by start date) + 6mo Active/Complete (by completion date)
--   - Default value for unmatched deployments: 'No Deployment'
--
-- Charts Supported:
--   - 4.1 Foundation Recipe Builds: COUNTD(customer_billing_id) by biweekly_period
--   - 4.3 Migration Recipe Builds: COUNTD(customer_billing_id) by biweekly_period
--
-- Join Path (matches old "Tenant Build with Deployments" datasource):
--   tenant_build -> sfdc_account_details (billing_id) -> sfdc_deployments (sf_account_id)
--
-- Key Features:
--   - build_status exposed as column (NOT filtered in query) for Tableau flexibility
--   - Column naming aligned with CT-TC datasources (deployment_* prefix)
--   - customer_sf_account_id included for Workday GO join capability
--   - Rolling 6-month window with semi-monthly grain
--
-- Deployment Scope:
--   - Active deployments started in last 6 months
--   - Active + Complete deployments with completion date in last 6 months
--
-- Filter Columns Available:
--   - enterprise_size_group, segment, super_industry (from sfdc_account_details)
--   - deployment_product_area, deployment_partner, deployment_phase, deployment_type (from sfdc_deployments)
--   - build_status (Completed/Not Completed)
--   - recipe_type (Foundation/Migration)
--
-- IMPORTANT: Use COUNTD(customer_billing_id) in Tableau, not COUNT(*)
--            Rows are expanded by deployment - same billing_id appears multiple times
--            if customer has multiple deployments in scope.
-- =============================================================================

-- =============================================================================
-- DATE RANGE CTE: Calculate 6-month semi-monthly window
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
)

-- =============================================================================
-- PART 1: FOUNDATION RECIPE BUILDS
-- =============================================================================
SELECT 
    'Foundation' AS recipe_type,
    tb.customer_billing_id,
    CAST(tb.wd_event_date AS DATE) AS wd_event_date,
    -- Semi-monthly period (biweekly_period)
    CASE 
        WHEN DAY(CAST(tb.wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(tb.wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(tb.wd_event_date AS DATE))))
    END AS biweekly_period,
    tb.build_status,
    sad.sf_account_id AS customer_sf_account_id,
    sad.account_name, -- Added here
    -- Filter columns from sfdc_account_details (aligned naming)
    COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sad.segment, 'Unknown') AS segment,
    COALESCE(sad.super_industry, 'Unknown') AS super_industry,
    -- Filter columns from sfdc_deployments (aligned naming with deployment_ prefix)
    COALESCE(d.product_area, 'No Deployment') AS deployment_product_area,
    COALESCE(d.priming_partner_name, 'No Deployment') AS deployment_partner,
    COALESCE(d.phase, 'No Deployment') AS deployment_phase,
    COALESCE(d.type, 'No Deployment') AS deployment_type,
    -- Deployment details (for reference/filtering)
    d.sf_deployment_id,
    DATE_TRUNC('month', CAST(d.deployment_start_date AS DATE)) AS deployment_month
FROM dw.swh.tenant_build tb
CROSS JOIN date_range dr
INNER JOIN dw.lookup_db.sfdc_account_details sad 
    ON tb.customer_billing_id = sad.billing_id
LEFT JOIN dw.lookup_db.sfdc_deployments d 
    ON sad.sf_account_id = d.customer_sf_account_id
    AND (
        (d.overall_status = 'Active'
         AND CAST(d.deployment_start_date AS DATE) >= dr.start_date)
        OR
        (d.overall_status IN ('Complete', 'Active')
         AND CAST(d.deployment_completion_date AS DATE) >= dr.start_date)
    )
WHERE tb.build_type = 'Foundation Tenant Build'
  -- Rolling 6-month semi-monthly window
  AND CAST(tb.wd_event_date AS DATE) >= dr.start_date
  AND CAST(tb.wd_event_date AS DATE) < dr.end_date

UNION ALL

-- =============================================================================
-- PART 2: MIGRATION RECIPE BUILDS
-- =============================================================================
SELECT 
    'Migration' AS recipe_type,
    tb.customer_billing_id,
    CAST(tb.wd_event_date AS DATE) AS wd_event_date,
    -- Semi-monthly period (biweekly_period)
    CASE 
        WHEN DAY(CAST(tb.wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(tb.wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(tb.wd_event_date AS DATE))))
    END AS biweekly_period,
    tb.build_status,
    sad.sf_account_id AS customer_sf_account_id,
    sad.account_name, -- Added here
    -- Filter columns from sfdc_account_details (aligned naming)
    COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sad.segment, 'Unknown') AS segment,
    COALESCE(sad.super_industry, 'Unknown') AS super_industry,
    -- Filter columns from sfdc_deployments (aligned naming with deployment_ prefix)
    COALESCE(d.product_area, 'No Deployment') AS deployment_product_area,
    COALESCE(d.priming_partner_name, 'No Deployment') AS deployment_partner,
    COALESCE(d.phase, 'No Deployment') AS deployment_phase,
    COALESCE(d.type, 'No Deployment') AS deployment_type,
    -- Deployment details (for reference/filtering)
    d.sf_deployment_id,
    DATE_TRUNC('month', CAST(d.deployment_start_date AS DATE)) AS deployment_month
FROM dw.swh.tenant_build tb
CROSS JOIN date_range dr
INNER JOIN dw.lookup_db.sfdc_account_details sad 
    ON tb.customer_billing_id = sad.billing_id
LEFT JOIN dw.lookup_db.sfdc_deployments d 
    ON sad.sf_account_id = d.customer_sf_account_id
    AND (
        (d.overall_status = 'Active'
         AND CAST(d.deployment_start_date AS DATE) >= dr.start_date)
        OR
        (d.overall_status IN ('Complete', 'Active')
         AND CAST(d.deployment_completion_date AS DATE) >= dr.start_date)
    )
WHERE tb.build_type = 'Migration Recipe'
  -- Rolling 6-month semi-monthly window
  AND CAST(tb.wd_event_date AS DATE) >= dr.start_date
  AND CAST(tb.wd_event_date AS DATE) < dr.end_date


-- =============================================================================
-- TABLEAU CALCULATED FIELDS
-- =============================================================================
-- 
-- Is Completed Build:
--   [build_status] = 'Completed'
--
-- Is Workday Go Customer? (after Redshift join):
--   NOT ISNULL([accountid])
--
-- Deployment Type Filter (LOD for Initial vs Subsequent):
--   { FIXED [customer_sf_account_id], [biweekly_period] :
--     MAX(IF [deployment_type] != 'Initial Deployment' 
--         THEN 'Subsequent Deployment' 
--         ELSE 'Initial Deployment' END)
--   }
--
-- =============================================================================


-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================
-- Run these in Trino to verify counts:
--
-- Foundation Builds by biweekly_period (Completed only):
-- SELECT biweekly_period, COUNT(DISTINCT customer_billing_id) 
-- FROM (<this_query>) 
-- WHERE recipe_type = 'Foundation' AND build_status = 'Completed'
-- GROUP BY biweekly_period
-- ORDER BY biweekly_period;
--
-- Migration Builds by biweekly_period (Completed only):
-- SELECT biweekly_period, COUNT(DISTINCT customer_billing_id) 
-- FROM (<this_query>) 
-- WHERE recipe_type = 'Migration' AND build_status = 'Completed'
-- GROUP BY biweekly_period
-- ORDER BY biweekly_period;
--
-- All Builds (including non-completed):
-- SELECT recipe_type, build_status, COUNT(DISTINCT customer_billing_id) as count
-- FROM (<this_query>) 
-- GROUP BY recipe_type, build_status;
--
-- =============================================================================
