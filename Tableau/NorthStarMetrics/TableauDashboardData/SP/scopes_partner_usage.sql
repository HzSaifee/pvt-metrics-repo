WITH Parameters AS (
    SELECT 
        DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE)) AS start_date,
        DATE_TRUNC('month', CURRENT_DATE) AS end_date
),

-- =============================================================================
-- 1. PARTNER UNIVERSE (The Denominator)
-- =============================================================================
active_deployments AS (
    SELECT DISTINCT 
        d.customer_sf_account_id,
        COALESCE(NULLIF(d.priming_partner_name, ''), 'Unknown Partner') AS partner_name
    FROM dw.lookup_db.sfdc_deployments d
    WHERE d.overall_status = 'Active'
      AND d.deployment_start_date > DATE '2023-01-01'
),

-- Enrich with Account Name from Account Details
partner_universe_named AS (
    SELECT 
        ad.partner_name,
        ad.customer_sf_account_id,
        COALESCE(sad.account_name, 'Unknown Account') AS account_name
    FROM active_deployments ad
    LEFT JOIN dw.lookup_db.sfdc_account_details sad 
        ON ad.customer_sf_account_id = sad.sf_account_id
),

-- =============================================================================
-- 2. RAW ACTIVITY DATA
-- =============================================================================
scopes_input_filtered AS (
    SELECT input_id, scope_external_id
    FROM dw.swh.scopes_input_type_metrics
    CROSS JOIN Parameters p
    WHERE wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

migration_filtered AS (
    SELECT event_id, source_object_id
    FROM dw.swh.migration_event_log
    CROSS JOIN Parameters p
    WHERE event_type = 'push_migration'
      AND wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- 2a. Change Tracker
ct_events AS (
    SELECT 
        ct.tenant,
        'Change Tracker' AS tool_type,
        CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END AS is_migrated
    FROM dw.swh.change_tracker_event_log ct
    CROSS JOIN Parameters p
    LEFT JOIN scopes_input_filtered s ON ct.change_tracker_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE ct.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND ct.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND ct.time >= p.start_date AND ct.time < p.end_date
      AND ct.user_type IN ('Customer', 'Implementer')
),

-- 2b. Tenant Compare
tc_events AS (
    SELECT 
        tc.tenant,
        'Tenant Compare' AS tool_type,
        CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END AS is_migrated
    FROM dw.swh.tenant_compare_event_log tc
    CROSS JOIN Parameters p
    LEFT JOIN scopes_input_filtered s ON tc.tenant_compare_scope_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE tc.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND tc.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND tc.time >= p.start_date AND tc.time < p.end_date
      AND tc.user_type IN ('Customer', 'Implementer')
),

-- 2c. Manual Scope
ms_events AS (
    SELECT 
        sm.tenant_name AS tenant,
        'Manual Scope' AS tool_type,
        CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END AS is_migrated
    FROM dw.swh.scopes_metrics sm
    CROSS JOIN Parameters p
    LEFT JOIN (SELECT DISTINCT scope_external_id FROM scopes_input_filtered) swi 
        ON sm.scope_external_id = swi.scope_external_id
    LEFT JOIN migration_filtered m ON sm.scope_external_id = m.source_object_id
    WHERE sm.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND sm.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
      AND sm.time >= p.start_date AND sm.time < p.end_date
      AND sm.user_type IN ('Customer', 'Implementer')
      AND swi.scope_external_id IS NULL 
),

raw_activity AS (
    SELECT * FROM ct_events
    UNION ALL
    SELECT * FROM tc_events
    UNION ALL
    SELECT * FROM ms_events
),

-- =============================================================================
-- 3. CONSOLIDATE ACTIVITY BY CUSTOMER & TOOL
-- Determine if a specific customer used a specific tool, and if they migrated.
-- =============================================================================
customer_tool_usage AS (
    SELECT 
        sfdc.sf_id AS account_id,
        r.tool_type,
        -- If ANY event for this customer/tool was a migration, flag as 1
        MAX(r.is_migrated) AS has_migrated_with_tool
    FROM raw_activity r
    INNER JOIN dw.lookup_db.sfdc_customer_tenants sfdc 
        ON LOWER(r.tenant) = LOWER(sfdc.tenant_name)
    GROUP BY 1, 2
)

-- =============================================================================
-- 4. FINAL OUTPUT: Partner + Customer + Tool
-- =============================================================================
SELECT 
    u.partner_name,
    u.customer_sf_account_id,
    UPPER(u.customer_sf_account_id) IN (SELECT UPPER(account_id) FROM dw.cdt.workday_go_accounts) AS go_customer,
    u.account_name,            -- For Display in Tableau
    
    -- Tool Info (NULL if Inactive)
    COALESCE(usage.tool_type, 'No Activity') AS tool_type,
    
    -- Flags (1/0) for Tableau Counting
    CASE WHEN usage.tool_type IS NOT NULL THEN 1 ELSE 0 END AS is_created,
    COALESCE(usage.has_migrated_with_tool, 0) AS is_migrated

FROM partner_universe_named u
LEFT JOIN customer_tool_usage usage 
    ON u.customer_sf_account_id = usage.account_id