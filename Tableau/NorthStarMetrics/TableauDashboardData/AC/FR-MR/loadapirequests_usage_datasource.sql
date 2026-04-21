-- =============================================================================
-- LoadApiRequests_Usage DATASOURCE
-- =============================================================================
-- Purpose: Powers LoadAPI migrateable request charts with account & deployment filters
--
-- Grain: One row per migrateable LoadAPI request per deployment
--   - Migrateable filter applied in SQL (type > component fallback)
--   - Rows expand per deployment; use COUNTD(request_id) in Tableau
--   - request_id assigned via ROW_NUMBER before deployment join (guaranteed unique)
--   - Customers with no matching deployment get one row with 'No Deployment' values
--
-- Account Resolution (94.9% match rate):
--   loadapirequests.customer -> sfdc_account_tenant_map.tenant_name -> sf_account_id
--   Unmatched tenants (~5%) show 'Unknown' for all filter columns
--
-- Deployment Scope:
--   - Active deployments started in last 6 months
--   - Complete/Active deployments with completion date in last 6 months
--
-- Date Range: Rolling 12 months (full completed months)
--
-- IMPORTANT: Use COUNTD([request_id]) in Tableau, not COUNT(*)
-- =============================================================================


-- =============================================================================
-- Step 1: Migrateable requests with unique request_id (before deployment join)
-- =============================================================================
WITH migrateable_requests AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY r.time, r.customer, r.correlation_id) AS request_id,
        DATE_TRUNC('month', r.time) AS month,
        r.customer,
        r.originator
    FROM dw.swh.loadapirequests r
    LEFT JOIN dw.cdt.implementation_types_detail t
        ON LOWER(TRIM(r.implementation_type)) = LOWER(TRIM(t.implementation_type))
    LEFT JOIN dw.cdt.implementation_component_details c
        ON LOWER(TRIM(r.implementation_component)) = LOWER(TRIM(c.component_name))
    WHERE r.wd_event_date >= CAST(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12' MONTH) AS VARCHAR)
      AND r.wd_event_date <  CAST(DATE_TRUNC('month', CURRENT_DATE) AS VARCHAR)
      AND COALESCE(
            NULLIF(LOWER(TRIM(t.migrateable)), ''),
            NULLIF(LOWER(TRIM(c.migrateable)), '')
          ) = 'migrateable'
),

-- =============================================================================
-- Step 2: Account lookup (tenant_name -> sf_account_id, 1:1 verified)
-- =============================================================================
account_lookup AS (
    SELECT DISTINCT tenant_name, sf_account_id
    FROM dw.lookup_db.sfdc_account_tenant_map
),

-- =============================================================================
-- Step 3: Recent deployments (pre-filtered for performance)
-- =============================================================================
recent_deployments AS (
    SELECT
        d.customer_sf_account_id,
        COALESCE(d.product_area, 'No Deployment') AS deployment_product_area,
        COALESCE(d.priming_partner_name, 'No Deployment') AS deployment_partner,
        COALESCE(d.phase, 'No Deployment') AS deployment_phase,
        COALESCE(d.type, 'No Deployment') AS deployment_type,
        d.overall_status AS deployment_status
    FROM dw.lookup_db.sfdc_deployments d
    WHERE (
        (d.overall_status = 'Active'
         AND CAST(d.deployment_start_date AS DATE) >= DATE_ADD('month', -6, DATE_TRUNC('month', CURRENT_DATE)))
        OR
        (d.overall_status IN ('Complete', 'Active')
         AND CAST(d.deployment_completion_date AS DATE) >= DATE_ADD('month', -6, DATE_TRUNC('month', CURRENT_DATE)))
    )
)

-- =============================================================================
-- Step 4: Join requests to account + deployment data
-- =============================================================================
SELECT
    mr.request_id,
    mr.month,
    mr.customer,
    mr.originator,
    al.sf_account_id,
    COALESCE(sad.account_name, 'Unknown') AS account_name,
    UPPER(al.sf_account_id) IN (SELECT UPPER(account_id) FROM dw.cdt.workday_go_accounts) AS go_customer,
    COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sad.segment, 'Unknown') AS segment,
    COALESCE(sad.super_industry, 'Unknown') AS super_industry,
    COALESCE(rd.deployment_product_area, 'No Deployment') AS deployment_product_area,
    COALESCE(rd.deployment_partner, 'No Deployment') AS deployment_partner,
    COALESCE(rd.deployment_phase, 'No Deployment') AS deployment_phase,
    COALESCE(rd.deployment_type, 'No Deployment') AS deployment_type,
    COALESCE(rd.deployment_status, 'No Deployment') AS deployment_status
FROM migrateable_requests mr
LEFT JOIN account_lookup al
    ON mr.customer = al.tenant_name
LEFT JOIN dw.lookup_db.sfdc_account_details sad
    ON al.sf_account_id = sad.sf_account_id
LEFT JOIN recent_deployments rd
    ON al.sf_account_id = rd.customer_sf_account_id


-- =============================================================================
-- TABLEAU USAGE
-- =============================================================================
--
-- Chart: LoadAPI Migrateable Requests by Originator
--   - X-axis: MONTH([month])
--   - Color/Detail: [originator]
--   - Measure: COUNTD([request_id])
--   - Filters: account_name, enterprise_size_group, segment, super_industry,
--              deployment_product_area, deployment_partner, deployment_phase,
--              deployment_type, deployment_status
--
-- =============================================================================


-- =============================================================================
-- ORIGINAL QUERY:
-- =============================================================================
-- WITH source_data AS (
--     SELECT 
--         DATE_TRUNC('month', r.time) AS month,
--         r.originator,
--         -- NULLIF ensures that if a mapping is just blank spaces, it treats it as NULL so it can fall back to the component
--         NULLIF(LOWER(TRIM(t.migrateable)), '') AS type_migrateable_flag,
--         NULLIF(LOWER(TRIM(c.migrateable)), '') AS comp_migrateable_flag
--     FROM dw.swh.loadapirequests r
--     LEFT JOIN dw.cdt.implementation_types_detail t 
--         ON LOWER(TRIM(r.implementation_type)) = LOWER(TRIM(t.implementation_type))
--     LEFT JOIN dw.cdt.implementation_component_details c 
--         ON LOWER(TRIM(r.implementation_component)) = LOWER(TRIM(c.component_name))
--     WHERE r.wd_event_date >= CAST(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12' MONTH) AS VARCHAR)
--       AND r.wd_event_date <  CAST(DATE_TRUNC('month', CURRENT_DATE) AS VARCHAR)
-- )
-- SELECT
--     month,
--     originator,
--     COUNT(*) AS request_count,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY month), 2) AS pct_of_month
-- FROM source_data
-- WHERE 
--     -- This evaluates Type first. If Type is NULL, it evaluates Component. 
--     -- It only keeps the record if the winning value is 'migrateable'.
--     COALESCE(type_migrateable_flag, comp_migrateable_flag) = 'migrateable'
-- GROUP BY
--     month,
--     originator
-- ORDER BY
--     month,
--     request_count DESC
-- =============================================================================