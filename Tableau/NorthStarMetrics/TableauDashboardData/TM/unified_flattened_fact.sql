-- =============================================================================
-- UNIFIED FLATTENED FACT TABLE
-- =============================================================================
-- Purpose: Single flat table for tool usage analysis and penetration calculations.
--          Each row = customer x deployment x tool event (x migration for migrated rows).
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
-- Grain: One row per tool event per migration per deployment per customer.
--        Tool events with no migration get one row (NULL migration columns).
--        Tool events with N migrations get N rows (same tool_instance_id,
--        different migration_id).
--        Migration-only rows (no tool event in window) get one row per migration
--        with biweekly_period=NULL, created_by=NULL (excluded from tool charts).
--        Customers with no tool events still get rows (NULL tool columns).
--        Accounts WITHOUT deployments get nth=0 only.
--        Accounts WITH qualifying deployments get nth=1,2,3...
--        Enrichment-only deployments get nth=NULL.
--
-- Event rows include ALL deployments (even excluded scopes) for filtering.
-- No-event rows exclude excluded-scope deployments (clean denominator).
-- Final SELECT uses CROSS JOIN row-type duplication + conditional LEFT JOIN
-- to produce event rows and denominator rows in a single pass, referencing
-- each CTE exactly once to avoid Trino stage explosion.
-- Denominator rows have NULL tool columns for Tableau filter stability.
--
-- Time window: Rolling 6-month parameterized biweekly grain.
--
-- Columns:
--   created_by               — who created/used the tool (from tool event log)
--   migrated_by              — who pushed the migration (from migration_event_log)
--   biweekly_period          — biweekly period of tool event
--   migration_biweekly_period — biweekly period of migration push
--
-- Tableau usage:
--   Adoption rate  = COUNTD(acct WHERE tool_category IS NOT NULL AND qualifies...)
--                    / COUNTD(acct WHERE is_active_customer)
--   Tool usage     = COUNTD(tool_instance_id) GROUP BY created_by, biweekly_period
--   Migration      = COUNTD(migration_id) GROUP BY migrated_by, migration_biweekly_period
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

-- Date-filtered scope-input linkage (for Adhoc anti-join within the rolling window).
-- input_id IS NOT NULL excludes rows with no tool linkage (blank input_type)
-- so those scopes can correctly fall through to the Adhoc path.
scopes_input_for_events AS (
    SELECT DISTINCT
        input_id,
        scope_external_id
    FROM dw.swh.scopes_input_type_metrics
    CROSS JOIN Parameters p
    WHERE input_id IS NOT NULL
      AND wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- Unfiltered scope-input linkage (for migration attribution regardless of tool creation date).
-- input_id IS NOT NULL for same reason as above.
scopes_input_for_migration AS (
    SELECT DISTINCT
        input_id,
        scope_external_id
    FROM dw.swh.scopes_input_type_metrics
    WHERE input_id IS NOT NULL
      AND wd_event_date IS NOT NULL
),

-- Partition-pruned migration_event_log (push migrations with Materialized Scopes)
-- Includes migrated_by so the migration's user_type flows to the final output
migration_filtered AS (
    SELECT
        m.event_id,
        m.source_object_id,
        m.migration_id,
        m.user_type AS migrated_by,
        m.cc_tenant AS tenant,
        CASE
            WHEN DAY(CAST(m.time AS DATE)) <= 15
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(m.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(m.time AS DATE))))
        END AS migration_biweekly_period
    FROM dw.swh.migration_event_log m
    CROSS JOIN Parameters p
    WHERE m.event_type = 'push_migration'
      AND m.source_object_type = 'Materialized Scope'
      AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND m.time >= p.start_date
      AND m.time < p.end_date
),

-- Distinct scope_external_ids linked to any tool input within the date window (Adhoc anti-join target)
scopes_with_input AS (
    SELECT DISTINCT scope_external_id
    FROM scopes_input_for_events
),

-- Distinct (tool_instance_id -> migration) links via unfiltered scope mapping.
-- Used by CT and TC events to expand to one row per migration.
-- DISTINCT prevents duplicate rows when one input_id maps to multiple scope_external_ids
-- that resolve to the same migration_id.
tool_migration_links AS (
    SELECT DISTINCT
        s.input_id AS tool_instance_id,
        m.migration_id,
        m.migrated_by,
        m.migration_biweekly_period
    FROM scopes_input_for_migration s
    INNER JOIN migration_filtered m
        ON s.scope_external_id = m.source_object_id
),

-- adhoc_migration_links removed: inlined into adhoc_scope_events as a direct
-- LEFT JOIN to migration_filtered, saving one CTE expansion of migration_filtered.


-- =============================================================================
-- LAYER 1: RAW TOOL EVENTS + MIGRATION-ONLY FALLBACK
-- =============================================================================
-- CT/TC/Adhoc: one row per tool event per migration (LEFT JOIN to migration links).
--   - Events with 0 migrations: 1 row, migration columns NULL
--   - Events with N migrations: N rows, same tool_instance_id, different migration_id
-- Migration-only: migrations not captured by CT/TC/Adhoc events (tool outside window,
--   no tool linkage, or orphan scopes). biweekly_period=NULL, created_by=NULL.
-- FR/MR: resolved via billing_id -> sfdc_account_details directly.
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
        ct.user_type AS created_by,
        ct.tenant,
        'Change Tracker' AS tool_category,
        ct.change_tracker_wid AS tool_instance_id,
        tml.migration_id,
        tml.migration_biweekly_period,
        tml.migrated_by,
        CASE WHEN tml.migration_id IS NOT NULL THEN 'Migrated' ELSE 'Created' END AS interaction_status,
        CAST(NULL AS VARCHAR) AS build_status
    FROM dw.swh.change_tracker_event_log ct
    CROSS JOIN Parameters p
    LEFT JOIN tool_migration_links tml
        ON ct.change_tracker_wid = tml.tool_instance_id
    WHERE ct.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND ct.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND ct.time >= p.start_date
      AND ct.time < p.end_date
      AND ct.user_type IN ('Customer', 'Implementer')
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
        tc.user_type AS created_by,
        tc.tenant,
        'Tenant Compare' AS tool_category,
        tc.tenant_compare_scope_wid AS tool_instance_id,
        tml.migration_id,
        tml.migration_biweekly_period,
        tml.migrated_by,
        CASE WHEN tml.migration_id IS NOT NULL THEN 'Migrated' ELSE 'Created' END AS interaction_status,
        CAST(NULL AS VARCHAR) AS build_status
    FROM dw.swh.tenant_compare_event_log tc
    CROSS JOIN Parameters p
    LEFT JOIN tool_migration_links tml
        ON tc.tenant_compare_scope_wid = tml.tool_instance_id
    WHERE tc.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND tc.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND tc.time >= p.start_date
      AND tc.time < p.end_date
      AND tc.user_type IN ('Customer', 'Implementer')
),

-- Adhoc Scope events (scopes NOT linked to CT/TC within the date window, raw tenant)
-- Migration link inlined: LEFT JOIN to migration_filtered directly instead of via
-- adhoc_migration_links CTE, avoiding an extra expansion of migration_filtered.
adhoc_scope_events AS (
    SELECT
        CASE
            WHEN DAY(CAST(sm.time AS DATE)) <= 15
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(sm.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(sm.time AS DATE))))
        END AS biweekly_period,
        sm.time AS interaction_exact_date,
        sm.user_type AS created_by,
        sm.tenant_name AS tenant,
        'Adhoc Scope' AS tool_category,
        sm.scope_external_id AS tool_instance_id,
        mf.migration_id,
        mf.migration_biweekly_period,
        mf.migrated_by,
        CASE WHEN mf.migration_id IS NOT NULL THEN 'Migrated' ELSE 'Created' END AS interaction_status,
        CAST(NULL AS VARCHAR) AS build_status
    FROM dw.swh.scopes_metrics sm
    CROSS JOIN Parameters p
    LEFT JOIN scopes_with_input swi ON sm.scope_external_id = swi.scope_external_id
    LEFT JOIN migration_filtered mf ON sm.scope_external_id = mf.source_object_id
    WHERE sm.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND sm.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND sm.time >= p.start_date
      AND sm.time < p.end_date
      AND sm.user_type IN ('Customer', 'Implementer')
      AND swi.scope_external_id IS NULL  -- anti-join: exclude scopes linked to CT/TC
),

-- Migration-only events: migrations in migration_filtered NOT already captured by
-- ct_events, tc_events, or adhoc_scope_events.  These are migrations where:
--   (a) the tool was created outside the 6-month window (CT/TC with no event in window), or
--   (b) the scope has no tool linkage (NULL input_id in scopes_input), or
--   (c) the scope exists in neither scopes_input nor scopes_metrics (orphan).
-- biweekly_period and created_by are NULL so these rows never appear in tool usage charts.
-- migration_biweekly_period and migrated_by have values for migration charts.
--
-- IMPORTANT: Anti-join checks use direct base table scans (partition-pruned) instead of
-- referencing ct_events/tc_events/adhoc_scope_events CTEs. Trino inlines CTEs at each
-- reference point, so re-referencing heavy event CTEs would double the stage count.
migration_only_events AS (
    SELECT
        CAST(NULL AS DATE) AS biweekly_period,
        CAST(NULL AS TIMESTAMP) AS interaction_exact_date,
        CAST(NULL AS VARCHAR) AS created_by,
        mf.tenant,
        COALESCE(scope_tool.tool_category, 'Adhoc Scope') AS tool_category,
        COALESCE(scope_tool.input_id, mf.source_object_id) AS tool_instance_id,
        mf.migration_id,
        mf.migration_biweekly_period,
        mf.migrated_by,
        'Migrated' AS interaction_status,
        CAST(NULL AS VARCHAR) AS build_status
    FROM migration_filtered mf
    LEFT JOIN (
        SELECT DISTINCT
            input_id,
            scope_external_id,
            CASE
                WHEN LOWER(input_type) LIKE '%change tracker%' THEN 'Change Tracker'
                WHEN LOWER(input_type) LIKE '%tenant compare%' THEN 'Tenant Compare'
            END AS tool_category
        FROM dw.swh.scopes_input_type_metrics
        WHERE input_id IS NOT NULL
          AND wd_event_date IS NOT NULL
    ) scope_tool ON mf.source_object_id = scope_tool.scope_external_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM dw.swh.change_tracker_event_log ct
        CROSS JOIN Parameters p
        INNER JOIN dw.swh.scopes_input_type_metrics si
            ON ct.change_tracker_wid = si.input_id
        WHERE si.scope_external_id = mf.source_object_id
          AND si.input_id IS NOT NULL
          AND si.wd_event_date IS NOT NULL
          AND ct.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
          AND ct.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
          AND ct.time >= p.start_date AND ct.time < p.end_date
          AND ct.user_type IN ('Customer', 'Implementer')
    )
    AND NOT EXISTS (
        SELECT 1
        FROM dw.swh.tenant_compare_event_log tc
        CROSS JOIN Parameters p
        INNER JOIN dw.swh.scopes_input_type_metrics si
            ON tc.tenant_compare_scope_wid = si.input_id
        WHERE si.scope_external_id = mf.source_object_id
          AND si.input_id IS NOT NULL
          AND si.wd_event_date IS NOT NULL
          AND tc.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
          AND tc.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
          AND tc.time >= p.start_date AND tc.time < p.end_date
          AND tc.user_type IN ('Customer', 'Implementer')
    )
    AND NOT EXISTS (
        SELECT 1
        FROM dw.swh.scopes_metrics sm
        CROSS JOIN Parameters p
        LEFT JOIN scopes_with_input swi
            ON sm.scope_external_id = swi.scope_external_id
        WHERE sm.scope_external_id = mf.source_object_id
          AND sm.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
          AND sm.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
          AND sm.time >= p.start_date AND sm.time < p.end_date
          AND sm.user_type IN ('Customer', 'Implementer')
          AND swi.scope_external_id IS NULL
    )
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
        CAST(NULL AS VARCHAR) AS migrated_by,
        'Built' AS interaction_status,
        tb.build_status,
        'Implementer' AS created_by
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
        raw_events.migrated_by,
        raw_events.interaction_status,
        raw_events.build_status,
        raw_events.created_by
    FROM (
        SELECT * FROM ct_events
        UNION ALL
        SELECT * FROM tc_events
        UNION ALL
        SELECT * FROM adhoc_scope_events
        UNION ALL
        SELECT * FROM migration_only_events
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
        migrated_by,
        interaction_status,
        build_status,
        created_by
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
        migrated_by,
        interaction_status,
        build_status,
        created_by
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
-- FINAL SELECT: Single-pass row-type duplication.
-- CROSS JOIN with 2-row helper ('event','denom') + conditional LEFT JOIN
-- so customer_deployment_base and all_tool_events are each referenced once.
-- Event rows:  row_type='event', tool columns from ate (ate IS NOT NULL)
-- Denom rows:  row_type='denom', tool columns NULL, filtered to active accounts
-- Tableau filters on created_by/tool_category must include NULL to preserve
-- denominator stability.
-- =============================================================================

SELECT
    CASE WHEN d.row_type = 'event' THEN ate.biweekly_period ELSE p.end_date END AS biweekly_period,
    cdb.customer_sf_account_id,
    CASE WHEN d.row_type = 'event' THEN COALESCE(ate.billing_id, cdb.billing_id) ELSE cdb.billing_id END AS billing_id,
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
    ate.migrated_by,
    ate.interaction_status,
    ate.interaction_exact_date,
    ate.build_status,
    ate.created_by,
    cdb.enterprise_size_group,
    cdb.segment,
    cdb.industry,
    cdb.super_industry,
    cdb.segment_size_l1
FROM customer_deployment_base cdb
CROSS JOIN Parameters p
CROSS JOIN (SELECT 'event' AS row_type UNION ALL SELECT 'denom' AS row_type) d
LEFT JOIN all_tool_events ate
    ON cdb.customer_sf_account_id = ate.customer_sf_account_id
    AND d.row_type = 'event'
WHERE (cdb.is_excluded_scope = FALSE OR cdb.nth_active_deployment = 0)
  AND (
    (d.row_type = 'event' AND ate.customer_sf_account_id IS NOT NULL)
    OR
    (d.row_type = 'denom' AND (cdb.is_active_customer = TRUE
                                OR cdb.has_active_deployment = TRUE))
  )
ORDER BY cdb.customer_sf_account_id, cdb.nth_active_deployment NULLS LAST, 1
