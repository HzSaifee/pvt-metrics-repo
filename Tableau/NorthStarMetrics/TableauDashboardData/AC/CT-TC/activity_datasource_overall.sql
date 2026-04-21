-- =============================================================================
-- AC_CT-TC_Activity OVERALL VIEW (Simplified Architecture)
-- =============================================================================
-- Purpose: Simplified query for OVERALL charts (Charts 1 & 3)
--          Each customer appears at ONE date only based on their status
--
-- Key Principle:
--   - If customer EVER migrated (any tool) → counted at FIRST migration date
--   - If customer NEVER migrated → counted at FIRST created date
--
-- Grain: One row per billing_id × user_type × deployment combination
--        All rows for a customer have the SAME biweekly_period
--
-- Charts Supported:
--   Chart 1: Cumulative Migrated Customers - Overall (stacked by LE/ME)
--   Chart 3: Created vs Migrated - Overall (stacked by status)
--
-- Tableau Usage:
--   - Filter by customer_status for migrated-only charts
--   - Color by enterprise_size_group OR customer_status
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
        END AS end_date,
        -- Shifted window for non-Launch Express Initial Deployments by 6 months based on Migration Recipe Usage from Initial Deployment Start Date
        DATE_TRUNC('month', DATE_ADD('month', -(6 + 6), CURRENT_DATE)) AS initial_start_date_ac,
        CASE 
            WHEN DAY(CURRENT_DATE) <= 15 
                THEN DATE_ADD('month', -6, DATE_TRUNC('month', CURRENT_DATE))
            ELSE DATE_ADD('month', -6, DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE)))
        END AS initial_end_date_ac
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
-- UNION: Subsequent + LE Initial (standard window, Active + Complete)
-- vs Non-LE Initial (shifted window, Active + Complete)
-- =============================================================================
account_deployments AS (
    -- Subsequent + Launch Express Initial: standard 6-month window, Active + Complete
    SELECT 
        customer_sf_account_id,
        COALESCE(NULLIF(product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(phase, ''), 'Unknown') AS deployment_phase,
        deployment_start_date
    FROM dw.lookup_db.sfdc_deployments
    CROSS JOIN Parameters p
    WHERE overall_status IN ('Active', 'Complete')
        AND (type != 'Initial Deployment' OR phase = 'Launch Express')
        AND deployment_start_date >= p.start_date
        AND deployment_start_date <= p.end_date
    UNION ALL
    -- Non-Launch Express Initial Deployments: shifted window, Active + Complete
    SELECT 
        customer_sf_account_id,
        COALESCE(NULLIF(product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(phase, ''), 'Unknown') AS deployment_phase,
        deployment_start_date
    FROM dw.lookup_db.sfdc_deployments
    CROSS JOIN Parameters p
    WHERE overall_status IN ('Active', 'Complete')
        AND type = 'Initial Deployment'
        AND (phase IS NULL OR phase != 'Launch Express')
        AND deployment_start_date >= p.initial_start_date_ac
        AND deployment_start_date <= p.initial_end_date_ac
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
        COALESCE(adc.deployment_product_area, 'No Deployment') AS deployment_product_area,
        COALESCE(adc.deployment_partner, 'No Deployment') AS deployment_partner,
        COALESCE(adc.deployment_type, 'No Deployment') AS deployment_type,
        COALESCE(adc.deployment_phase, 'No Deployment') AS deployment_phase,
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
-- STEP 8: Calculate FIRST CREATED Date per Customer (Overall - any tool)
-- =============================================================================
first_created_overall AS (
    SELECT 
        billing_id,
        user_type,
        MIN(biweekly_period) AS first_created_period
    FROM enriched_events
    GROUP BY 1, 2
),

-- =============================================================================
-- STEP 9: Calculate FIRST MIGRATED Date per Customer (Overall - any tool)
-- =============================================================================
first_migrated_overall AS (
    SELECT 
        billing_id,
        user_type,
        MIN(biweekly_period) AS first_migrated_period
    FROM enriched_events
    WHERE has_migration_link = 1
    GROUP BY 1, 2
),

-- =============================================================================
-- STEP 10: Determine THE ONE EVENT DATE per Customer
-- Each customer appears at exactly ONE period:
--   - If they EVER migrated → first_migrated_period
--   - If they NEVER migrated → first_created_period
-- =============================================================================
customer_event_date AS (
    SELECT 
        fc.billing_id,
        fc.user_type,
        fc.first_created_period,
        fm.first_migrated_period,
        -- THE event date for this customer
        COALESCE(fm.first_migrated_period, fc.first_created_period) AS event_period,
        -- Customer status (final, not evolving)
        CASE 
            WHEN fm.first_migrated_period IS NOT NULL THEN 'Migrated'
            ELSE 'Created Only'
        END AS customer_status
    FROM first_created_overall fc
    LEFT JOIN first_migrated_overall fm 
        ON fc.billing_id = fm.billing_id 
        AND fc.user_type = fm.user_type
),

-- =============================================================================
-- STEP 11: Get Distinct Customer Dimension Combinations
-- We need deployment attributes for filtering, but customer appears at ONE date
-- =============================================================================
customer_dimensions AS (
    SELECT DISTINCT
        e.billing_id,
        e.user_type,
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
        ced.event_period AS biweekly_period,
        ced.billing_id,
        ced.user_type,
        ced.customer_status,
        ced.first_created_period,
        ced.first_migrated_period,
        cd.sf_account_id,
        cd.account_name,
        cd.tenant_env_type,
        cd.enterprise_size_group,
        cd.deployment_bucket,
        cd.segment,
        cd.industry,
        cd.super_industry,
        cd.segment_size_l1,
        cd.deployment_product_area,
        cd.deployment_partner,
        cd.deployment_type,
        cd.deployment_phase,
        cd.latest_deployment_start
    FROM customer_event_date ced
    INNER JOIN customer_dimensions cd 
        ON ced.billing_id = cd.billing_id 
        AND ced.user_type = cd.user_type
)

-- =============================================================================
-- FINAL OUTPUT
-- =============================================================================
SELECT 
    biweekly_period,
    billing_id,
    user_type,
    customer_status,
    first_created_period,
    first_migrated_period,
    sf_account_id,
    UPPER(sf_account_id) IN (SELECT UPPER(account_id) FROM dw.cdt.workday_go_accounts) AS go_customer,
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
ORDER BY biweekly_period DESC, user_type, billing_id
