-- =============================================================================
-- FR-MR USAGE DATASOURCE
-- =============================================================================
-- Purpose: Powers Foundation and Migration Usage % charts (4.2 and 4.4)
--
-- Both parts share the same structure:
--   - Grain: One row per deployment, Usage % counted at CUSTOMER level
--   - X-axis: completion_date (deployment_completion_date), monthly in Tableau
--   - used_recipe_flag = 1 when CUSTOMER ever had a completed recipe build
--   - Usage %: COUNTD(IF flag THEN customer_sf_account_id) / COUNTD(customer_sf_account_id)
--   - Full date range (filter to last 12 months in Tableau)
--
-- PART 1 — Foundation Recipe:
--   - Phase exclusion list (penetration_datasource_overall aligned)
--   - Build check: Foundation Tenant Build (customer-level, any-time)
--
-- PART 2 — Migration Recipe:
--   - Phase IN ('Your Way', 'Launch') only
--   - Build check: Migration Recipe (customer-level, any-time)
--   - Filter by deployment_type in Tableau as needed
--
-- Common Filters:
--   - overall_status IN ('Complete', 'Active')
--   - Product area exclusion list
--   - Segment exclusion via sfdc_account_details
--   - deployment_completion_date IS NOT NULL
--
-- Join Path:
--   sfdc_deployments -> sfdc_account_tenant_map -> tenant_build
--   sfdc_deployments -> sfdc_account_details (filter columns + segment filter)
--
-- Filter Columns Available:
--   - enterprise_size_group, segment, super_industry, account_name
--   - deployment_product_area, deployment_partner, deployment_phase, deployment_type
--   - recipe_type (Foundation/Migration)
-- =============================================================================


-- =============================================================================
-- PART 1: FOUNDATION RECIPE USAGE
-- =============================================================================

WITH foundation_recipe_deployments AS (
    SELECT DISTINCT
        d.sf_deployment_id,
        d.customer_sf_account_id,
        d.type,
        d.deployment_start_date,
        CAST(d.deployment_completion_date AS DATE) AS completion_date,
        DATE_TRUNC('month', CAST(d.deployment_completion_date AS DATE)) AS completion_month,
        COALESCE(d.product_area, 'Unknown') AS deployment_product_area,
        COALESCE(d.priming_partner_name, 'Unknown') AS deployment_partner,
        COALESCE(d.phase, 'Unknown') AS deployment_phase
    FROM dw.lookup_db.sfdc_deployments d
    INNER JOIN dw.lookup_db.sfdc_account_details sad
        ON d.customer_sf_account_id = sad.sf_account_id
    WHERE d.overall_status IN ('Complete', 'Active')
      AND d.phase NOT IN (
          'Adhoc', 'Customer Enablement', 'Customer Led',
          'Peakon First', 'Phase - X - Planning', 'Phase X - Peakon',
          'Phase X - Planning', 'Phase X - Sourcing', 'Phase X - VNDLY',
          'Planning First', 'Sourcing First', 'VNDLY First'
      )
      AND COALESCE(d.product_area, '') NOT IN (
          'Adaptive Planning', 'HiredScore', 'Planning', 'VNDLY',
          'Workday HiredScore', 'Workday Peakon Employee Voice',
          'Workday Success Plans', 'Workday VNDLY'
      )
      AND sad.segment NOT IN ('CSD EMEA', 'Specialized', 'US Federal')
      AND d.deployment_completion_date IS NOT NULL
),

foundation_recipe_usage AS (
    SELECT DISTINCT d.customer_sf_account_id
    FROM foundation_recipe_deployments d
    INNER JOIN dw.lookup_db.sfdc_account_tenant_map atm
        ON d.customer_sf_account_id = atm.sf_account_id
    INNER JOIN dw.swh_raw.tenant_build tb
        ON atm.billing_id = tb.customer_billing_id
    WHERE tb.build_type = 'Foundation Tenant Build'
      AND tb.build_status = 'Completed'
      AND tb.wd_event_date IS NOT NULL
),

-- =============================================================================
-- PART 2: MIGRATION RECIPE USAGE
-- =============================================================================

migration_recipe_deployments AS (
    SELECT DISTINCT
        d.sf_deployment_id,
        d.customer_sf_account_id,
        d.type,
        d.deployment_start_date,
        CAST(d.deployment_completion_date AS DATE) AS completion_date,
        DATE_TRUNC('month', CAST(d.deployment_completion_date AS DATE)) AS completion_month,
        COALESCE(d.product_area, 'Unknown') AS deployment_product_area,
        COALESCE(d.priming_partner_name, 'Unknown') AS deployment_partner,
        COALESCE(d.phase, 'Unknown') AS deployment_phase
    FROM dw.lookup_db.sfdc_deployments d
    INNER JOIN dw.lookup_db.sfdc_account_details sad
        ON d.customer_sf_account_id = sad.sf_account_id
    WHERE d.overall_status IN ('Complete', 'Active')
      AND d.phase IN ('Your Way', 'Launch')
      AND COALESCE(d.product_area, '') NOT IN (
          'Adaptive Planning', 'HiredScore', 'Planning', 'VNDLY',
          'Workday HiredScore', 'Workday Peakon Employee Voice',
          'Workday Success Plans', 'Workday VNDLY'
      )
      AND sad.segment NOT IN ('CSD EMEA', 'Specialized', 'US Federal')
      AND d.deployment_completion_date IS NOT NULL
),

migration_recipe_usage AS (
    SELECT DISTINCT d.customer_sf_account_id
    FROM migration_recipe_deployments d
    INNER JOIN dw.lookup_db.sfdc_account_tenant_map atm
        ON d.customer_sf_account_id = atm.sf_account_id
    INNER JOIN dw.swh_raw.tenant_build tb
        ON atm.billing_id = tb.customer_billing_id
    WHERE tb.build_type = 'Migration Recipe'
      AND tb.build_status = 'Completed'
      AND tb.wd_event_date IS NOT NULL
)

-- =============================================================================
-- FINAL OUTPUT: Combined Foundation + Migration
-- =============================================================================

-- Foundation Recipe Deployments
SELECT
    'Foundation' AS recipe_type,
    frd.sf_deployment_id,
    frd.customer_sf_account_id,
    UPPER(frd.customer_sf_account_id) IN (SELECT UPPER(account_id) FROM dw.cdt.workday_go_accounts) AS go_customer,
    frd.completion_month,
    frd.completion_date,
    frd.deployment_product_area,
    frd.deployment_partner,
    frd.deployment_phase,
    COALESCE(frd.type, 'Unknown') AS deployment_type,
    COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sad.segment, 'Unknown') AS segment,
    COALESCE(sad.super_industry, 'Unknown') AS super_industry,
    COALESCE(sad.account_name, 'Unknown') AS account_name,
    CASE WHEN fru.customer_sf_account_id IS NOT NULL THEN 1 ELSE 0 END AS used_recipe_flag
FROM foundation_recipe_deployments frd
LEFT JOIN foundation_recipe_usage fru
    ON frd.customer_sf_account_id = fru.customer_sf_account_id
LEFT JOIN dw.lookup_db.sfdc_account_details sad
    ON frd.customer_sf_account_id = sad.sf_account_id

UNION ALL

-- Migration Recipe Deployments
SELECT
    'Migration' AS recipe_type,
    mrd.sf_deployment_id,
    mrd.customer_sf_account_id,
    UPPER(mrd.customer_sf_account_id) IN (SELECT UPPER(account_id) FROM dw.cdt.workday_go_accounts) AS go_customer,
    mrd.completion_month,
    mrd.completion_date,
    mrd.deployment_product_area,
    mrd.deployment_partner,
    mrd.deployment_phase,
    COALESCE(mrd.type, 'Unknown') AS deployment_type,
    COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sad.segment, 'Unknown') AS segment,
    COALESCE(sad.super_industry, 'Unknown') AS super_industry,
    COALESCE(sad.account_name, 'Unknown') AS account_name,
    CASE WHEN mru.customer_sf_account_id IS NOT NULL THEN 1 ELSE 0 END AS used_recipe_flag
FROM migration_recipe_deployments mrd
LEFT JOIN migration_recipe_usage mru
    ON mrd.customer_sf_account_id = mru.customer_sf_account_id
LEFT JOIN dw.lookup_db.sfdc_account_details sad
    ON mrd.customer_sf_account_id = sad.sf_account_id


-- =============================================================================
-- TABLEAU CALCULATED FIELDS
-- =============================================================================
--
-- Usage Percentage (unified for both Foundation and Migration):
--   COUNTD(IF [used_recipe_flag] = 1 THEN [customer_sf_account_id] END)
--   / COUNTD([customer_sf_account_id])
--
-- Deployment Type Filter (LOD for Initial vs Subsequent):
--   { FIXED [customer_sf_account_id], [completion_month] :
--     MAX(IF [deployment_type] != 'Initial Deployment'
--         THEN 'Subsequent Deployment'
--         ELSE 'Initial Deployment' END)
--   }
--
-- Is Workday Go Customer?
--   Use the [go_customer] column (BOOLEAN) — computed server-side from cdt.workday_go_accounts.
--
-- =============================================================================


-- =============================================================================
-- TABLEAU USAGE NOTES
-- =============================================================================
--
-- Chart 4.2 - Foundation Recipe Usage %:
--   - Filter: [recipe_type] = 'Foundation'
--   - Filter: date range to last 12 months
--   - X-axis: MONTH([completion_date])
--   - Measure: Usage Percentage calculated field
--
-- Chart 4.4 - Migration Recipe Usage %:
--   - Filter: [recipe_type] = 'Migration'
--   - Filter: [deployment_type] = 'Initial Deployment' (optional)
--   - Filter: date range to last 12 months
--   - X-axis: MONTH([completion_date])
--   - Measure: Usage Percentage calculated field
--
-- =============================================================================


-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================
--
-- Foundation Usage % by completion month (counts customers):
-- SELECT
--     completion_month,
--     COUNT(DISTINCT customer_sf_account_id) AS total_customers,
--     COUNT(DISTINCT CASE WHEN used_recipe_flag = 1 THEN customer_sf_account_id END) AS used_customers,
--     CAST(COUNT(DISTINCT CASE WHEN used_recipe_flag = 1 THEN customer_sf_account_id END) AS DOUBLE)
--         / COUNT(DISTINCT customer_sf_account_id) AS usage_percentage
-- FROM (<this_query>)
-- WHERE recipe_type = 'Foundation'
-- GROUP BY completion_month
-- ORDER BY completion_month;
--
-- Migration Usage % by completion month (counts customers):
-- SELECT
--     completion_month,
--     COUNT(DISTINCT customer_sf_account_id) AS total_customers,
--     COUNT(DISTINCT CASE WHEN used_recipe_flag = 1 THEN customer_sf_account_id END) AS used_customers,
--     CAST(COUNT(DISTINCT CASE WHEN used_recipe_flag = 1 THEN customer_sf_account_id END) AS DOUBLE)
--         / COUNT(DISTINCT customer_sf_account_id) AS usage_percentage
-- FROM (<this_query>)
-- WHERE recipe_type = 'Migration'
-- GROUP BY completion_month
-- ORDER BY completion_month;
--
-- =============================================================================
