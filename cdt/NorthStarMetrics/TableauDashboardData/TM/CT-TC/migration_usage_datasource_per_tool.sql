-- =============================================================================
-- Migration Usage PER-TOOL VIEW
-- =============================================================================
-- Purpose: Row-level query to count unique migrations per tool in Tableau.
--
-- Key Principles:
--   - Driven by `migration_event_log` (Push Migrations, Materialized Scopes).
--   - Categorizes migrations based on `scopes_input_type_metrics`.
--   - If a scope has multiple input tools (e.g., CT and TC), it generates 
--     one row per tool so both get credit in Tableau.
--   - Unmatched Materialized Scopes default to 'Adhoc Scope'.
--
-- Grain: One row per migration_id × tool_type.
-- =============================================================================

WITH Parameters AS (
    SELECT 
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS start_date,
        -- End at the start of current biweekly period (excludes current incomplete period)
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
-- PRE-FILTER: migration_event_log (Push Migrations + Materialized Scopes)
-- =============================================================================
migration_filtered AS (
    SELECT 
        m.migration_id,
        m.event_id,
        m.cc_billing_id AS billing_id,
        m.cc_tenant AS tenant,
        m.user_type,
        m.source_object_id,
        m.time,
        CASE 
            WHEN DAY(CAST(m.time AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(m.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(m.time AS DATE))))
        END AS biweekly_period
    FROM dw.swh.migration_event_log m
    CROSS JOIN Parameters p
    WHERE m.event_type = 'push_migration'
      AND m.source_object_type = 'Materialized Scope'
      AND m.wd_event_date IS NOT NULL
      AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND m.time >= p.start_date
      AND m.time < p.end_date
      AND m.user_type IN ('Customer', 'Implementer')
),

-- =============================================================================
-- PRE-FILTER: scopes_input_type_metrics to categorize Tool Type
-- =============================================================================
scopes_input_filtered AS (
    SELECT DISTINCT
        s.scope_external_id,
        CASE 
            -- Adjust these strings if the actual input_type names differ slightly
            WHEN LOWER(s.input_type) LIKE '%change tracker%' THEN 'Change Tracker'
            WHEN LOWER(s.input_type) LIKE '%tenant compare%' THEN 'Tenant Compare'
            ELSE 'Other Tool' 
        END AS tool_type
    FROM dw.swh.scopes_input_type_metrics s
    CROSS JOIN Parameters p
    WHERE s.wd_event_date IS NOT NULL
      AND s.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND s.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- =============================================================================
-- STEP 1: Map Migrations to Tools
-- If a migration doesn't link to scopes_input_type_metrics, it falls to Adhoc
-- =============================================================================
migration_tool_mapping AS (
    SELECT DISTINCT
        m.migration_id,
        m.billing_id,
        m.tenant,
        m.user_type,
        m.biweekly_period,
        COALESCE(s.tool_type, 'Adhoc Scope') AS tool_type
    FROM migration_filtered m
    LEFT JOIN scopes_input_filtered s
        ON m.source_object_id = s.scope_external_id
),

-- =============================================================================
-- PRE-AGGREGATE: Active Deployments per Account for Enrichment
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
-- STEP 2: Pre-calculate Phase X Deployment Flag per Account
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
-- STEP 3: Final Output - Enrich Row-Level Migrations with SFDC Dimensions
-- =============================================================================
final_output AS (
    SELECT 
        mt.biweekly_period,
        mt.migration_id,
        mt.billing_id,
        mt.user_type,
        mt.tool_type,
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
        COALESCE(adc.deployment_phase, 'No Active Deployment') AS deployment_phase
    FROM migration_tool_mapping mt
    INNER JOIN dw.lookup_db.sfdc_customer_tenants sfdc 
        ON LOWER(mt.tenant) = LOWER(sfdc.tenant_name)
    LEFT JOIN dw.lookup_db.sfdc_account_details sad 
        ON sfdc.sf_id = sad.sf_account_id
    LEFT JOIN phase_x_accounts px 
        ON sfdc.sf_id = px.customer_sf_account_id
    LEFT JOIN account_deployment_combos adc
        ON sfdc.sf_id = adc.customer_sf_account_id
    WHERE sfdc.billing_id IS NOT NULL
)

-- =============================================================================
-- FINAL OUTPUT
-- =============================================================================
SELECT 
    biweekly_period,
    migration_id,
    billing_id,
    user_type,
    tool_type,
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