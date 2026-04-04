-- =============================================================================
-- UNIFIED FLATTENED FACT TABLE
-- =============================================================================
-- Purpose: Single flat table for tool usage analysis and penetration calculations.
--          Each row = customer x deployment x tool event.
--          Data population (enrichment) is SEPARATE from qualification logic:
--            - Deployment details populated broadly for all relevant deployments
--            - Qualification flags computed independently per row
--
-- Flags:
--   is_active_customer    — account-level, tenant + deployment-based qualification
--   has_active_deployment — account-level, TRUE if any qualifying+non-excluded deploy
--   qualifies_as_active   — per-deployment, strict status-based penetration criteria
--   is_excluded_scope     — per-deployment, phase/product area exclusion marker
--
-- Tools: Change Tracker, Tenant Compare, Adhoc Scope, Foundation Recipe,
--        Migration Recipe  (tool_group: CT_TC_AS, FR_MR)
--
-- Grain: One row per tool event per deployment per customer.
--        Customers with no tool events still get rows (NULL tool columns).
--        Accounts WITHOUT deployments get nth=0 only.
--        Accounts WITH qualifying deployments get nth=1,2,3...
--        Enrichment-only deployments get nth=NULL.
--
-- Event rows include ALL deployments (even excluded scopes) for filtering.
-- No-event rows exclude excluded-scope deployments (clean denominator).
--
-- Time window: Rolling 6-month parameterized biweekly grain.
--
-- Tableau usage:
--   Adoption rate  = COUNTD(acct WHERE tool_category IS NOT NULL AND qualifies...)
--                    / COUNTD(acct WHERE is_active_customer)
--   Usage volume   = COUNT(tool events) GROUP BY biweekly_period, tool_group
--   Migration      = COUNTD(migration_id) GROUP BY migration_biweekly_period
-- =============================================================================


-- =============================================================================
-- LAYER 0: PARAMETERS & SHARED FILTERS
-- =============================================================================

WITH Parameters AS (
    SELECT
        -- Rolling 6-month window (change -6 to adjust)
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS start_date,
        CASE
            WHEN DAY(CURRENT_DATE) <= 15
                THEN DATE_TRUNC('month', CURRENT_DATE)
            ELSE DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE))
        END AS end_date,
        -- Shifted window for non-Launch Express Initial Deployments (6 additional months back)
        DATE_TRUNC('month', DATE_ADD('month', -(6 + 6), CURRENT_DATE)) AS initial_start_date_ac,
        CASE
            WHEN DAY(CURRENT_DATE) <= 15
                THEN DATE_ADD('month', -6, DATE_TRUNC('month', CURRENT_DATE))
            ELSE DATE_ADD('month', -6, DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE)))
        END AS initial_end_date_ac
),

-- Partition-pruned scopes_input_type_metrics (links CT/TC wids to scope_external_ids)
scopes_input_filtered AS (
    SELECT DISTINCT
        input_id,
        scope_external_id
    FROM dw.swh.scopes_input_type_metrics
    CROSS JOIN Parameters p
    WHERE wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- Partition-pruned migration_event_log (push migrations with Materialized Scopes)
-- Extended with migration_id and migration_biweekly_period for verification
migration_filtered AS (
    SELECT
        m.event_id,
        m.source_object_id,
        m.migration_id,
        CASE
            WHEN DAY(CAST(m.time AS DATE)) <= 15
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(m.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(m.time AS DATE))))
        END AS migration_biweekly_period
    FROM dw.swh.migration_event_log m
    CROSS JOIN Parameters p
    WHERE m.event_type = 'push_migration'
      AND m.source_object_type = 'Materialized Scope'
      AND m.wd_event_date IS NOT NULL
      AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND m.time >= p.start_date
      AND m.time < p.end_date
),

-- Distinct scope_external_ids linked to CT/TC (anti-join target for adhoc exclusion)
scopes_with_input AS (
    SELECT DISTINCT scope_external_id
    FROM scopes_input_filtered
),


-- =============================================================================
-- LAYER 1: RAW TOOL EVENTS (4 CTEs)
-- CT/TC/Adhoc output raw tenant (resolved to sf_account_id later in one join)
-- FR/MR resolve via billing_id -> sfdc_account_details directly
-- =============================================================================

-- Change Tracker events (raw tenant, not yet resolved to customer_sf_account_id)
ct_events AS (
    SELECT
        CASE
            WHEN DAY(CAST(ct.time AS DATE)) <= 15
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(ct.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(ct.time AS DATE))))
        END AS biweekly_period,
        ct.time AS interaction_exact_date,
        ct.user_type,
        ct.tenant,
        'Change Tracker' AS tool_category,
        ct.change_tracker_wid AS tool_instance_id,
        MAX(m.migration_id) AS migration_id,
        MAX(m.migration_biweekly_period) AS migration_biweekly_period,
        CASE WHEN MAX(m.event_id) IS NOT NULL THEN 'Migrated' ELSE 'Created' END AS interaction_status,
        CAST(NULL AS VARCHAR) AS build_status
    FROM dw.swh.change_tracker_event_log ct
    CROSS JOIN Parameters p
    LEFT JOIN scopes_input_filtered s ON ct.change_tracker_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE ct.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND ct.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND ct.time >= p.start_date
      AND ct.time < p.end_date
      AND ct.user_type IN ('Customer', 'Implementer')
    GROUP BY 1, 2, 3, 4, 5, 6, 10
),

-- Tenant Compare events (raw tenant, not yet resolved)
tc_events AS (
    SELECT
        CASE
            WHEN DAY(CAST(tc.time AS DATE)) <= 15
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(tc.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(tc.time AS DATE))))
        END AS biweekly_period,
        tc.time AS interaction_exact_date,
        tc.user_type,
        tc.tenant,
        'Tenant Compare' AS tool_category,
        tc.tenant_compare_scope_wid AS tool_instance_id,
        MAX(m.migration_id) AS migration_id,
        MAX(m.migration_biweekly_period) AS migration_biweekly_period,
        CASE WHEN MAX(m.event_id) IS NOT NULL THEN 'Migrated' ELSE 'Created' END AS interaction_status,
        CAST(NULL AS VARCHAR) AS build_status
    FROM dw.swh.tenant_compare_event_log tc
    CROSS JOIN Parameters p
    LEFT JOIN scopes_input_filtered s ON tc.tenant_compare_scope_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE tc.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND tc.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND tc.time >= p.start_date
      AND tc.time < p.end_date
      AND tc.user_type IN ('Customer', 'Implementer')
    GROUP BY 1, 2, 3, 4, 5, 6, 10
),

-- Adhoc Scope events (scopes NOT linked to CT/TC, raw tenant)
adhoc_scope_events AS (
    SELECT
        CASE
            WHEN DAY(CAST(sm.time AS DATE)) <= 15
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(sm.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(sm.time AS DATE))))
        END AS biweekly_period,
        sm.time AS interaction_exact_date,
        sm.user_type,
        sm.tenant_name AS tenant,
        'Adhoc Scope' AS tool_category,
        sm.scope_external_id AS tool_instance_id,
        MAX(m.migration_id) AS migration_id,
        MAX(m.migration_biweekly_period) AS migration_biweekly_period,
        CASE WHEN MAX(m.event_id) IS NOT NULL THEN 'Migrated' ELSE 'Created' END AS interaction_status,
        CAST(NULL AS VARCHAR) AS build_status
    FROM dw.swh.scopes_metrics sm
    CROSS JOIN Parameters p
    LEFT JOIN scopes_with_input swi ON sm.scope_external_id = swi.scope_external_id
    LEFT JOIN migration_filtered m ON sm.scope_external_id = m.source_object_id
    WHERE sm.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND sm.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND sm.time >= p.start_date
      AND sm.time < p.end_date
      AND sm.user_type IN ('Customer', 'Implementer')
      AND swi.scope_external_id IS NULL  -- anti-join: exclude scopes linked to CT/TC
    GROUP BY 1, 2, 3, 4, 5, 6, 10
),

-- Foundation Recipe + Migration Recipe events (combined, resolved via billing_id)
fr_mr_events AS (
    SELECT
        sad.sf_account_id AS customer_sf_account_id,
        tb.customer_billing_id AS billing_id,
        CASE
            WHEN DAY(CAST(tb.wd_event_date AS DATE)) <= 15
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(tb.wd_event_date AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(tb.wd_event_date AS DATE))))
        END AS biweekly_period,
        tb.time AS interaction_exact_date,
        CASE
            WHEN tb.build_type = 'Foundation Tenant Build' THEN 'Foundation Recipe'
            ELSE 'Migration Recipe'
        END AS tool_category,
        CASE
            WHEN tb.build_type = 'Foundation Tenant Build'
                THEN tb.customer_billing_id || '_' || tb.recipe_name || '_' || CAST(tb.time AS VARCHAR)
            ELSE tb.recipe_name
        END AS tool_instance_id,
        CAST(NULL AS VARCHAR) AS migration_id,
        CAST(NULL AS DATE) AS migration_biweekly_period,
        'Built' AS interaction_status,
        tb.build_status,
        'Implementer' AS user_type
    FROM dw.swh.tenant_build tb
    CROSS JOIN Parameters p
    INNER JOIN dw.lookup_db.sfdc_account_details sad
        ON tb.customer_billing_id = sad.billing_id
    WHERE tb.build_type IN ('Foundation Tenant Build', 'Migration Recipe')
      AND CAST(tb.wd_event_date AS DATE) >= p.start_date
      AND CAST(tb.wd_event_date AS DATE) < p.end_date
),


-- =============================================================================
-- LAYER 2: UNIFIED EVENTS
-- Single sfdc_customer_tenants join for CT/TC/Adhoc, then UNION with FR/MR
-- =============================================================================

-- Resolve CT/TC/Adhoc raw tenant to customer_sf_account_id (one LOWER join)
ct_tc_adhoc_resolved AS (
    SELECT
        sct.sf_id AS customer_sf_account_id,
        sct.billing_id,
        raw_events.biweekly_period,
        raw_events.interaction_exact_date,
        raw_events.tool_category,
        raw_events.tool_instance_id,
        raw_events.migration_id,
        raw_events.migration_biweekly_period,
        raw_events.interaction_status,
        raw_events.build_status,
        raw_events.user_type
    FROM (
        SELECT * FROM ct_events
        UNION ALL
        SELECT * FROM tc_events
        UNION ALL
        SELECT * FROM adhoc_scope_events
    ) raw_events
    INNER JOIN dw.lookup_db.sfdc_customer_tenants sct
        ON LOWER(raw_events.tenant) = LOWER(sct.tenant_name)
    WHERE sct.billing_id IS NOT NULL
),

-- All tool events unified
all_tool_events AS (
    SELECT
        customer_sf_account_id,
        billing_id,
        biweekly_period,
        interaction_exact_date,
        tool_category,
        tool_instance_id,
        migration_id,
        migration_biweekly_period,
        interaction_status,
        build_status,
        user_type
    FROM ct_tc_adhoc_resolved

    UNION ALL

    SELECT
        customer_sf_account_id,
        billing_id,
        biweekly_period,
        interaction_exact_date,
        tool_category,
        tool_instance_id,
        migration_id,
        migration_biweekly_period,
        interaction_status,
        build_status,
        user_type
    FROM fr_mr_events
),


-- =============================================================================
-- LAYER 3: CUSTOMER UNIVERSE & DEPLOYMENTS
-- =============================================================================

-- All relevant deployments: broader set with per-row qualification flags.
-- Data population (enrichment) is separate from qualification logic.
-- Phase/product exclusions are flags, not WHERE filters, so all deployment
-- data is available for Tableau filtering on event rows.
all_relevant_deployments AS (
    -- Part 1: Subsequent + Launch Express Initial — standard window, Active + Complete
    SELECT
        d.customer_sf_account_id,
        d.sf_deployment_id,
        d.deployment_start_date,
        COALESCE(NULLIF(d.phase, ''), 'Unknown') AS deployment_phase,
        COALESCE(NULLIF(d.product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(d.priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(d.type, ''), 'Unknown') AS deployment_type,
        d.overall_status AS deployment_overall_status,
        CASE WHEN d.overall_status = 'Active' THEN TRUE ELSE FALSE END AS qualifies_as_active,
        CASE
            WHEN COALESCE(d.phase, '') IN (
                'Adhoc', 'Customer Enablement', 'Customer Led',
                'Peakon First', 'Phase - X - Planning',
                'Phase X - Peakon', 'Phase X - Planning', 'Phase X - Sourcing',
                'Phase X - VNDLY', 'Planning First', 'Sourcing First', 'VNDLY First'
            ) THEN TRUE
            WHEN COALESCE(d.product_area, '') IN (
                'Adaptive Planning', 'HiredScore', 'Planning', 'VNDLY',
                'Workday HiredScore', 'Workday Peakon Employee Voice',
                'Workday Success Plans', 'Workday VNDLY'
            ) THEN TRUE
            ELSE FALSE
        END AS is_excluded_scope
    FROM dw.lookup_db.sfdc_deployments d
    CROSS JOIN Parameters p
    LEFT JOIN dw.lookup_db.sfdc_account_details sad
        ON d.customer_sf_account_id = sad.sf_account_id
    WHERE d.overall_status IN ('Active', 'Complete')
      AND (d.type != 'Initial Deployment' OR d.phase = 'Launch Express')
      AND COALESCE(sad.segment, '') NOT IN (
          'CSD EMEA',
          'Specialized',
          'US Federal'
      )
      AND d.deployment_start_date >= p.start_date
      AND d.deployment_start_date <= p.end_date

    UNION ALL

    -- Part 2: Non-Launch Express Initial — shifted window, Active + Complete
    -- Both Active and Complete qualify in the shifted window
    SELECT
        d.customer_sf_account_id,
        d.sf_deployment_id,
        d.deployment_start_date,
        COALESCE(NULLIF(d.phase, ''), 'Unknown') AS deployment_phase,
        COALESCE(NULLIF(d.product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(d.priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(d.type, ''), 'Unknown') AS deployment_type,
        d.overall_status AS deployment_overall_status,
        TRUE AS qualifies_as_active,
        CASE
            WHEN COALESCE(d.phase, '') IN (
                'Adhoc', 'Customer Enablement', 'Customer Led',
                'Peakon First', 'Phase - X - Planning',
                'Phase X - Peakon', 'Phase X - Planning', 'Phase X - Sourcing',
                'Phase X - VNDLY', 'Planning First', 'Sourcing First', 'VNDLY First'
            ) THEN TRUE
            WHEN COALESCE(d.product_area, '') IN (
                'Adaptive Planning', 'HiredScore', 'Planning', 'VNDLY',
                'Workday HiredScore', 'Workday Peakon Employee Voice',
                'Workday Success Plans', 'Workday VNDLY'
            ) THEN TRUE
            ELSE FALSE
        END AS is_excluded_scope
    FROM dw.lookup_db.sfdc_deployments d
    CROSS JOIN Parameters p
    LEFT JOIN dw.lookup_db.sfdc_account_details sad
        ON d.customer_sf_account_id = sad.sf_account_id
    WHERE d.overall_status IN ('Active', 'Complete')
      AND d.type = 'Initial Deployment'
      AND (d.phase IS NULL OR d.phase != 'Launch Express')
      AND COALESCE(sad.segment, '') NOT IN (
          'CSD EMEA',
          'Specialized',
          'US Federal'
      )
      AND d.deployment_start_date >= p.initial_start_date_ac
      AND d.deployment_start_date <= p.initial_end_date_ac
),

-- Deployment-based qualification for active customer determination (no date filter).
-- A customer qualifies if they have NO deployments, or at least one deployment
-- with a valid phase, valid product area, and Active/Complete status.
ac_qualification_deployments AS (
    SELECT DISTINCT
        d.customer_sf_account_id,
        COALESCE(NULLIF(d.phase, ''), 'Unknown') AS deployment_phase,
        COALESCE(NULLIF(d.product_area, ''), '') AS deployment_product_area,
        COALESCE(d.overall_status, 'Unknown') AS deployment_overall_status
    FROM dw.lookup_db.sfdc_deployments d
),

-- Active customer accounts (account-level flag, qualified by tenant + deployment criteria)
active_customer_accounts AS (
    SELECT DISTINCT
        sad.sf_account_id AS customer_sf_account_id
    FROM dw.lookup_db.sfdc_customer_tenants sct
    CROSS JOIN Parameters p
    INNER JOIN dw.lookup_db.sfdc_account_details sad
        ON sct.sf_id = sad.sf_account_id
    INNER JOIN dw.lookup_db.sfdc_customer_account_tenants scat
        ON sct.sf_id = scat.sf_account_id
        AND sct.tenant_name = scat.tenant_name
    LEFT JOIN ac_qualification_deployments aqd
        ON sad.sf_account_id = aqd.customer_sf_account_id
    WHERE sct.tenant_type = 'Production'
      AND sct.status = 'Active'
      AND sct.tenant_start_date <= p.end_date
      AND (sct.tenant_expire_date IS NULL OR sct.tenant_expire_date >= p.end_date)
      AND scat.tenant_prefix IS NOT NULL
      AND sad.assumed_enterprise_go_live_date IS NOT NULL
      AND sad.segment NOT IN (
          'CSD EMEA',
          'Specialized',
          'US Federal'
      )
      AND (
          aqd.customer_sf_account_id IS NULL
          OR (
              aqd.deployment_phase NOT IN (
                  'Adhoc', 'Peakon First', 'Phase - X - Planning',
                  'Phase X - Peakon', 'Phase X - Planning',
                  'Phase X - Sourcing', 'Phase X - VNDLY',
                  'Planning First', 'Sourcing First', 'VNDLY First'
              )
              AND aqd.deployment_product_area NOT IN (
                  'Adaptive Planning', 'HiredScore', 'Planning', 'VNDLY',
                  'Workday HiredScore', 'Workday Peakon Employee Voice',
                  'Workday Success Plans', 'Workday VNDLY'
              )
              AND aqd.deployment_overall_status IN ('Active', 'Complete')
          )
      )
),

-- Numbered deployments with qualification-aware numbering.
-- nth_active_deployment: 1,2,3... for qualifying+non-excluded; NULL for enrichment-only; 0 for no-deployment.
-- has_active_deployment: customer-level flag (TRUE if any qualifying+non-excluded deployment exists).
numbered_deployments AS (
    -- Real deployments (all from all_relevant_deployments, with conditional numbering)
    SELECT
        adr.customer_sf_account_id,
        CASE
            WHEN adr.qualifies_as_active = TRUE AND adr.is_excluded_scope = FALSE
            THEN CAST(SUM(CASE WHEN adr.qualifies_as_active = TRUE AND adr.is_excluded_scope = FALSE THEN 1 ELSE 0 END)
                 OVER (PARTITION BY adr.customer_sf_account_id
                       ORDER BY adr.deployment_start_date ASC, adr.sf_deployment_id ASC
                       ROWS UNBOUNDED PRECEDING) AS BIGINT)
            ELSE NULL
        END AS nth_active_deployment,
        MAX(CASE WHEN adr.qualifies_as_active = TRUE AND adr.is_excluded_scope = FALSE THEN 1 ELSE 0 END)
            OVER (PARTITION BY adr.customer_sf_account_id) > 0 AS has_active_deployment,
        adr.sf_deployment_id,
        adr.deployment_start_date,
        adr.deployment_phase,
        adr.deployment_product_area,
        adr.deployment_partner,
        adr.deployment_type,
        adr.deployment_overall_status,
        adr.qualifies_as_active,
        adr.is_excluded_scope
    FROM all_relevant_deployments adr

    UNION ALL

    -- Synthetic nth=0 for accounts with no non-excluded deployments in the window.
    -- Covers both: (a) accounts with truly no deployments, and (b) accounts whose
    -- ALL in-window deployments are is_excluded_scope=TRUE. Without this, group (b)
    -- would vanish from Part 2 output since the is_excluded_scope=FALSE filter
    -- drops all their deployment rows, losing their is_active_customer contribution.
    SELECT
        cu.customer_sf_account_id,
        CAST(0 AS BIGINT) AS nth_active_deployment,
        FALSE AS has_active_deployment,
        CAST(NULL AS VARCHAR) AS sf_deployment_id,
        CAST(NULL AS DATE) AS deployment_start_date,
        CAST(NULL AS VARCHAR) AS deployment_phase,
        CAST(NULL AS VARCHAR) AS deployment_product_area,
        CAST(NULL AS VARCHAR) AS deployment_partner,
        CAST(NULL AS VARCHAR) AS deployment_type,
        CAST(NULL AS VARCHAR) AS deployment_overall_status,
        FALSE AS qualifies_as_active,
        FALSE AS is_excluded_scope
    FROM (
        SELECT DISTINCT customer_sf_account_id FROM all_tool_events
        UNION
        SELECT customer_sf_account_id FROM active_customer_accounts
        UNION
        SELECT DISTINCT customer_sf_account_id FROM all_relevant_deployments
    ) cu
    WHERE NOT EXISTS (
        SELECT 1 FROM all_relevant_deployments adr
        WHERE adr.customer_sf_account_id = cu.customer_sf_account_id
          AND adr.is_excluded_scope = FALSE
    )
),

-- Customer deployment base enriched with is_active_customer + SFDC dimensions.
-- Passes through qualification flags from numbered_deployments for downstream use.
customer_deployment_base AS (
    SELECT
        nd.customer_sf_account_id,
        nd.nth_active_deployment,
        nd.has_active_deployment,
        nd.sf_deployment_id,
        nd.deployment_start_date,
        nd.deployment_phase,
        nd.deployment_product_area,
        nd.deployment_partner,
        nd.deployment_type,
        nd.deployment_overall_status,
        nd.qualifies_as_active,
        nd.is_excluded_scope,
        CASE WHEN ac.customer_sf_account_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_active_customer,
        COALESCE(sad.billing_id, 'Unknown') AS billing_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1
    FROM numbered_deployments nd
    LEFT JOIN active_customer_accounts ac
        ON nd.customer_sf_account_id = ac.customer_sf_account_id
    LEFT JOIN dw.lookup_db.sfdc_account_details sad
        ON nd.customer_sf_account_id = sad.sf_account_id
)


-- =============================================================================
-- FINAL SELECT: INNER JOIN for event rows + NOT EXISTS for no-event rows
-- =============================================================================

-- Part 1: Customers WITH tool events — ALL deployments included for enrichment/filtering
SELECT
    ate.biweekly_period,
    cdb.customer_sf_account_id,
    COALESCE(ate.billing_id, cdb.billing_id) AS billing_id,
    cdb.account_name,
    cdb.is_active_customer,
    cdb.has_active_deployment,
    cdb.nth_active_deployment,
    cdb.sf_deployment_id,
    cdb.deployment_start_date,
    cdb.deployment_phase,
    cdb.deployment_product_area,
    cdb.deployment_partner,
    cdb.deployment_type,
    cdb.deployment_overall_status,
    cdb.qualifies_as_active,
    cdb.is_excluded_scope,
    ate.tool_category,
    CASE
        WHEN ate.tool_category IN ('Change Tracker', 'Tenant Compare', 'Adhoc Scope') THEN 'CT_TC_AS'
        WHEN ate.tool_category IN ('Foundation Recipe', 'Migration Recipe') THEN 'FR_MR'
        ELSE NULL
    END AS tool_group,
    ate.tool_instance_id,
    ate.migration_id,
    ate.migration_biweekly_period,
    ate.interaction_status,
    ate.interaction_exact_date,
    ate.build_status,
    ate.user_type,
    cdb.enterprise_size_group,
    cdb.segment,
    cdb.industry,
    cdb.super_industry,
    cdb.segment_size_l1
FROM customer_deployment_base cdb
INNER JOIN all_tool_events ate
    ON cdb.customer_sf_account_id = ate.customer_sf_account_id

UNION ALL

-- Part 2: Customers WITHOUT tool events — only non-excluded deployments (denominator rows)
SELECT
    p.end_date AS biweekly_period,
    cdb.customer_sf_account_id,
    cdb.billing_id,
    cdb.account_name,
    cdb.is_active_customer,
    cdb.has_active_deployment,
    cdb.nth_active_deployment,
    cdb.sf_deployment_id,
    cdb.deployment_start_date,
    cdb.deployment_phase,
    cdb.deployment_product_area,
    cdb.deployment_partner,
    cdb.deployment_type,
    cdb.deployment_overall_status,
    cdb.qualifies_as_active,
    cdb.is_excluded_scope,
    CAST(NULL AS VARCHAR) AS tool_category,
    CAST(NULL AS VARCHAR) AS tool_group,
    CAST(NULL AS VARCHAR) AS tool_instance_id,
    CAST(NULL AS VARCHAR) AS migration_id,
    CAST(NULL AS DATE) AS migration_biweekly_period,
    CAST(NULL AS VARCHAR) AS interaction_status,
    CAST(NULL AS TIMESTAMP) AS interaction_exact_date,
    CAST(NULL AS VARCHAR) AS build_status,
    ut.user_type,
    cdb.enterprise_size_group,
    cdb.segment,
    cdb.industry,
    cdb.super_industry,
    cdb.segment_size_l1
FROM customer_deployment_base cdb
CROSS JOIN Parameters p
CROSS JOIN (VALUES ('Customer'), ('Implementer')) AS ut(user_type)
WHERE NOT EXISTS (
    SELECT 1 FROM all_tool_events ate
    WHERE ate.customer_sf_account_id = cdb.customer_sf_account_id
)
AND cdb.is_excluded_scope = FALSE
ORDER BY customer_sf_account_id, nth_active_deployment NULLS LAST, biweekly_period
