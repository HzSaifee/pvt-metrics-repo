-- =============================================================================
-- AC_CT-TC_Activity PER-TOOL VIEW (Simplified Architecture)
-- =============================================================================
-- Purpose: Simplified query for PER-TOOL charts (Charts 2 & 4)
--          Each customer-tool combination appears at ONE date only
--
-- Key Principle:
--   - If customer migrated with THIS TOOL → counted at FIRST migration date for this tool
--   - If customer never migrated with THIS TOOL → counted at FIRST created date for this tool
--
-- Grain: One row per billing_id × user_type × tool_type × deployment combination
--        All rows for a customer-tool have the SAME biweekly_period
--
-- Charts Supported:
--   Chart 2: Cumulative Migrated Customers - Per Tool (stacked by LE/ME)
--   Chart 4: Created vs Migrated - Per Tool (stacked by status)
--
-- Tableau Usage:
--   - Filter: tool_type = [single selection only]
--   - Filter by customer_status for migrated-only charts
--   - Color by enterprise_size_group OR customer_status
--
-- IMPORTANT: This query is designed for SINGLE tool selection only.
--            Multi-tool selection may produce unexpected results.
--
-- =============================================================================

WITH Parameters AS (
    SELECT 
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS start_date,
        -- End at the start of current biweekly period (excludes current incomplete period)
        -- If day <= 15: current period started on 1st, so end_date = 1st of month
        -- If day > 15: current period started on 16th, so end_date = 16th of month
        CASE 
            WHEN DAY(CURRENT_DATE) <= 15 
                THEN DATE_TRUNC('month', CURRENT_DATE)
            ELSE DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE))
        END AS end_date
),

-- =============================================================================
-- PRE-FILTER: scopes_input_type_metrics with partition pruning
-- =============================================================================
scopes_input_filtered AS (
    SELECT 
        input_id,
        scope_external_id
    FROM dw.swh.scopes_input_type_metrics
    CROSS JOIN Parameters p
    WHERE wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- =============================================================================
-- PRE-FILTER: migration_event_log with partition pruning (push_migration only)
-- =============================================================================
migration_filtered AS (
    SELECT 
        event_id,
        source_object_id
    FROM dw.swh.migration_event_log
    CROSS JOIN Parameters p
    WHERE event_type = 'push_migration'
      AND wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- =============================================================================
-- PRE-AGGREGATE: Active Deployments per Account
-- =============================================================================
account_deployments AS (
    SELECT 
        customer_sf_account_id,
        COALESCE(NULLIF(product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(phase, ''), 'Unknown') AS deployment_phase,
        deployment_start_date
    FROM dw.lookup_db.sfdc_deployments
    WHERE (overall_status = 'Active'
      OR deployment_completion_date BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6' MONTH) AND CURRENT_DATE
      )
      AND deployment_start_date >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '2' YEAR)
),

account_deployment_combos AS (
    SELECT DISTINCT
        customer_sf_account_id,
        deployment_product_area,
        deployment_partner,
        deployment_type,
        deployment_phase,
        MAX(deployment_start_date) AS latest_deployment_start
    FROM account_deployments
    GROUP BY 1, 2, 3, 4, 5
),

-- =============================================================================
-- STEP 1: Extract Change Tracker Events with Migration Link Status
-- =============================================================================
change_tracker_events AS (
    SELECT 
        CASE 
            WHEN DAY(CAST(ct.time AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(ct.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(ct.time AS DATE))))
        END AS biweekly_period,
        ct.user_type,
        ct.tenant,
        ct.change_tracker_wid,
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM dw.swh.change_tracker_event_log ct
    CROSS JOIN Parameters p
    LEFT JOIN scopes_input_filtered s ON ct.change_tracker_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE 
        ct.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND ct.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        AND ct.time >= p.start_date
        AND ct.time < p.end_date
        AND ct.user_type IN ('Customer', 'Implementer')
    GROUP BY 1, 2, 3, 4
),

-- =============================================================================
-- STEP 2: Extract Tenant Compare Events with Migration Link Status
-- =============================================================================
tenant_compare_events AS (
    SELECT 
        CASE 
            WHEN DAY(CAST(tc.time AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(tc.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(tc.time AS DATE))))
        END AS biweekly_period,
        tc.user_type,
        tc.tenant,
        tc.tenant_compare_scope_wid,
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM dw.swh.tenant_compare_event_log tc
    CROSS JOIN Parameters p
    LEFT JOIN scopes_input_filtered s ON tc.tenant_compare_scope_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE 
        tc.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND tc.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        AND tc.time >= p.start_date
        AND tc.time < p.end_date
        AND tc.user_type IN ('Customer', 'Implementer')
    GROUP BY 1, 2, 3, 4
),

-- =============================================================================
-- STEP 3: Identify Scopes with CT/TC Input (for Adhoc Scope exclusion)
-- =============================================================================
scopes_with_input AS (
    SELECT DISTINCT scope_external_id
    FROM scopes_input_filtered
),

-- =============================================================================
-- STEP 4: Extract Adhoc Scope Events with Migration Link Status
-- =============================================================================
manual_scope_events AS (
    SELECT 
        CASE 
            WHEN DAY(CAST(sm.time AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(sm.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(sm.time AS DATE))))
        END AS biweekly_period,
        sm.user_type,
        sm.tenant_name AS tenant,
        sm.scope_external_id,
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM dw.swh.scopes_metrics sm
    CROSS JOIN Parameters p
    LEFT JOIN scopes_with_input swi ON sm.scope_external_id = swi.scope_external_id
    LEFT JOIN migration_filtered m ON sm.scope_external_id = m.source_object_id
    WHERE 
        sm.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND sm.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        AND sm.time >= p.start_date
        AND sm.time < p.end_date
        AND sm.user_type IN ('Customer', 'Implementer')
        AND swi.scope_external_id IS NULL
    GROUP BY 1, 2, 3, 4
),

-- =============================================================================
-- STEP 5: Combine All Tool Events
-- =============================================================================
combined_tool_events AS (
    SELECT biweekly_period, user_type, tenant, 'Change Tracker' AS tool_type, has_migration_link
    FROM change_tracker_events
    UNION ALL
    SELECT biweekly_period, user_type, tenant, 'Tenant Compare' AS tool_type, has_migration_link
    FROM tenant_compare_events
    UNION ALL
    SELECT biweekly_period, user_type, tenant, 'Adhoc Scope' AS tool_type, has_migration_link
    FROM manual_scope_events
),

-- =============================================================================
-- STEP 6: Pre-calculate Phase X Deployment Flag per Account
-- =============================================================================
phase_x_accounts AS (
    SELECT 
        customer_sf_account_id,
        1 AS has_phase_x
    FROM dw.lookup_db.sfdc_deployments
    WHERE type != 'Initial Deployment'
    GROUP BY 1
),

-- =============================================================================
-- STEP 7: Enrich Events with Dimensions
-- =============================================================================
enriched_events AS (
    SELECT 
        cte.biweekly_period,
        cte.user_type,
        cte.tool_type,
        cte.has_migration_link,
        sfdc.billing_id,
        sfdc.tenant_type AS tenant_env_type,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        sad.sf_account_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        CASE 
            WHEN px.has_phase_x = 1 THEN 'Phase X Deployment'
            ELSE 'Initial Deployment'
        END AS deployment_bucket,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1,
        COALESCE(adc.deployment_product_area, 'No Active Deployment') AS deployment_product_area,
        COALESCE(adc.deployment_partner, 'No Active Deployment') AS deployment_partner,
        COALESCE(adc.deployment_type, 'No Active Deployment') AS deployment_type,
        COALESCE(adc.deployment_phase, 'No Active Deployment') AS deployment_phase,
        adc.latest_deployment_start
    FROM combined_tool_events cte
    INNER JOIN dw.lookup_db.sfdc_customer_tenants sfdc 
        ON LOWER(cte.tenant) = LOWER(sfdc.tenant_name)
    LEFT JOIN dw.lookup_db.sfdc_account_details sad 
        ON sfdc.sf_id = sad.sf_account_id
    LEFT JOIN phase_x_accounts px 
        ON sfdc.sf_id = px.customer_sf_account_id
    LEFT JOIN account_deployment_combos adc
        ON sfdc.sf_id = adc.customer_sf_account_id
    WHERE sfdc.billing_id IS NOT NULL
),

-- =============================================================================
-- STEP 8: Calculate FIRST CREATED Date per Customer-Tool
-- =============================================================================
first_created_per_tool AS (
    SELECT 
        billing_id,
        user_type,
        tool_type,
        MIN(biweekly_period) AS first_created_period
    FROM enriched_events
    GROUP BY 1, 2, 3
),

-- =============================================================================
-- STEP 9: Calculate FIRST MIGRATED Date per Customer-Tool
-- =============================================================================
first_migrated_per_tool AS (
    SELECT 
        billing_id,
        user_type,
        tool_type,
        MIN(biweekly_period) AS first_migrated_period
    FROM enriched_events
    WHERE has_migration_link = 1
    GROUP BY 1, 2, 3
),

-- =============================================================================
-- STEP 10: Determine THE ONE EVENT DATE per Customer-Tool
-- Each customer-tool appears at exactly ONE period:
--   - If they migrated with this tool → first_migrated_period for this tool
--   - If they never migrated with this tool → first_created_period for this tool
-- =============================================================================
customer_tool_event_date AS (
    SELECT 
        fc.billing_id,
        fc.user_type,
        fc.tool_type,
        fc.first_created_period,
        fm.first_migrated_period,
        -- THE event date for this customer-tool
        COALESCE(fm.first_migrated_period, fc.first_created_period) AS event_period,
        -- Customer status for THIS TOOL (not evolving)
        CASE 
            WHEN fm.first_migrated_period IS NOT NULL THEN 'Migrated'
            ELSE 'Created Only'
        END AS customer_status
    FROM first_created_per_tool fc
    LEFT JOIN first_migrated_per_tool fm 
        ON fc.billing_id = fm.billing_id 
        AND fc.user_type = fm.user_type
        AND fc.tool_type = fm.tool_type
),

-- =============================================================================
-- STEP 11: Get Distinct Customer-Tool Dimension Combinations
-- =============================================================================
customer_tool_dimensions AS (
    SELECT DISTINCT
        e.billing_id,
        e.user_type,
        e.tool_type,
        e.sf_account_id,
        e.account_name,
        e.tenant_env_type,
        e.enterprise_size_group,
        e.deployment_bucket,
        e.segment,
        e.industry,
        e.super_industry,
        e.segment_size_l1,
        e.deployment_product_area,
        e.deployment_partner,
        e.deployment_type,
        e.deployment_phase,
        e.latest_deployment_start
    FROM enriched_events e
),

-- =============================================================================
-- STEP 12: Final Output - Join Event Date with Dimensions
-- =============================================================================
final_output AS (
    SELECT 
        cted.event_period AS biweekly_period,
        cted.billing_id,
        cted.user_type,
        cted.tool_type,
        cted.customer_status,
        cted.first_created_period,
        cted.first_migrated_period,
        ctd.sf_account_id,
        ctd.account_name,
        ctd.tenant_env_type,
        ctd.enterprise_size_group,
        ctd.deployment_bucket,
        ctd.segment,
        ctd.industry,
        ctd.super_industry,
        ctd.segment_size_l1,
        ctd.deployment_product_area,
        ctd.deployment_partner,
        ctd.deployment_type,
        ctd.deployment_phase,
        ctd.latest_deployment_start
    FROM customer_tool_event_date cted
    INNER JOIN customer_tool_dimensions ctd 
        ON cted.billing_id = ctd.billing_id 
        AND cted.user_type = ctd.user_type
        AND cted.tool_type = ctd.tool_type
)

-- =============================================================================
-- FINAL OUTPUT
-- =============================================================================
SELECT 
    biweekly_period,
    billing_id,
    user_type,
    tool_type,
    customer_status,
    first_created_period,
    first_migrated_period,
    sf_account_id,
    account_name,
    tenant_env_type,
    enterprise_size_group,
    deployment_bucket,
    segment,
    industry,
    super_industry,
    segment_size_l1,
    deployment_product_area,
    deployment_partner,
    deployment_type,
    deployment_phase
FROM final_output
ORDER BY biweekly_period DESC, user_type, tool_type, billing_id
